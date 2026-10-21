/*
 * Copyright (c) 2026, The Hecaton Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.org.hecaton.persistence.internal.store;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static uk.org.hecaton.persistence.internal.store.ChunkFormat.CLEAR;
import static uk.org.hecaton.persistence.internal.store.ChunkFormat.HEADER_SIZE;
import static uk.org.hecaton.persistence.internal.store.ChunkFormat.PUT;
import static uk.org.hecaton.persistence.internal.store.ChunkFormat.REMOVE;

/**
 * The log of one partition: an ordered list of chunk files, the last of which is appended to,
 * and an index of the live records (see {@code docs/design/persistence.md}).
 * <p>
 * Not thread-safe: used by the partition's thread (or, while loading, by one loader thread).
 */
public final class PartitionLog implements Closeable {

    private static final byte[] EMPTY = new byte[0];
    private static final int MIN_BUFFER = 1 << 12;

    private final Path dir;
    private final LogSettings settings;
    /** All chunks by id; the last one is the active chunk. */
    private final TreeMap<Long, Chunk> chunks = new TreeMap<>();
    /** Per structure: key → location of its live record. */
    private final Map<Integer, Map<KeyBytes, Location>> index = new HashMap<>();
    private Chunk active;
    private FileChannel activeChannel;
    private ByteBuffer buffer = ByteBuffer.allocate(MIN_BUFFER);
    private long nextSeq = 1;
    private boolean unforced;
    private boolean closed;

    private PartitionLog(Path dir, LogSettings settings) {
        this.dir = dir;
        this.settings = settings;
    }

    /**
     * Opens the log in {@code dir}, creating it if needed, and reads its index. A torn record at the
     * end of the log is truncated.
     *
     * @throws PersistenceStoreException if the log cannot be read
     */
    public static PartitionLog open(Path dir, LogSettings settings) {
        PartitionLog log = new PartitionLog(dir, settings);
        try {
            Files.createDirectories(dir);
            log.load();
            log.openActive();
        } catch (IOException e) {
            log.closeQuietly();
            throw new PersistenceStoreException("Cannot open the log in " + dir, e);
        } catch (RuntimeException e) {
            log.closeQuietly();
            throw e;
        }
        return log;
    }

    /** Stores {@code value} for {@code key}. */
    public void put(int structureId, byte[] key, byte[] value) {
        KeyBytes keyBytes = new KeyBytes(key);
        Location location = append(PUT, structureId, key, value);
        active.live += location.size;
        Location previous = index.computeIfAbsent(structureId, id -> new HashMap<>()).put(keyBytes, location);
        if (previous != null) {
            release(previous);
        }
        rollIfFull();
    }

    /** Removes the entry of {@code key}, if there is one. */
    public void remove(int structureId, byte[] key) {
        Map<KeyBytes, Location> entries = index.get(structureId);
        Location previous = entries == null ? null : entries.get(new KeyBytes(key));
        if (previous == null) {
            return;
        }
        append(REMOVE, structureId, key, EMPTY);
        entries.remove(new KeyBytes(key));
        release(previous);
        rollIfFull();
    }

    /** Removes all entries of the structure. */
    public void clear(int structureId) {
        Map<KeyBytes, Location> entries = index.get(structureId);
        if (entries == null || entries.isEmpty()) {
            index.remove(structureId);
            return;
        }
        append(CLEAR, structureId, EMPTY, EMPTY);
        index.remove(structureId);
        for (Location location : entries.values()) {
            release(location);
        }
        rollIfFull();
    }

    /** Forces the written records to disk. */
    public void force() {
        if (unforced) {
            try {
                activeChannel.force(false);
            } catch (IOException e) {
                throw new PersistenceStoreException("Cannot force the log in " + dir + " to disk", e);
            }
            unforced = false;
        }
    }

    /** Number of entries of the structure. */
    public int size(int structureId) {
        Map<KeyBytes, Location> entries = index.get(structureId);
        return entries == null ? 0 : entries.size();
    }

    /** Structures with entries. */
    public Set<Integer> structures() {
        return Set.copyOf(index.keySet());
    }

    /** Calls the visitor for every live entry, oldest first. */
    public void replay(LogVisitor visitor) {
        try {
            for (Chunk chunk : List.copyOf(chunks.values())) {
                try (ChunkReader reader = new ChunkReader(chunk.path, true)) {
                    reader.readHeader();
                    while (reader.next()) {
                        if (reader.type == PUT && isLive(reader)) {
                            visitor.visit(reader.structureId, reader.key, reader.value);
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot read the log in " + dir, e);
        }
    }

    /** Bytes of the chunk files. */
    public long totalBytes() {
        return chunks.values().stream().mapToLong(chunk -> chunk.size).sum();
    }

    /** Bytes of the live records. */
    public long liveBytes() {
        return chunks.values().stream().mapToLong(chunk -> chunk.live).sum();
    }

    /** Number of chunk files. */
    public int chunkCount() {
        return chunks.size();
    }

    /**
     * Copies the log to {@code target} as it is now: the closed chunks, which do not change, as hard
     * links where the file system allows; the written part of the active chunk as a copy.
     */
    public void backupTo(Path target) {
        try {
            Files.createDirectories(target);
            force();
            for (Chunk chunk : chunks.values()) {
                Path copy = target.resolve(chunk.path.getFileName());
                if (chunk == active) {
                    try (FileChannel in = FileChannel.open(chunk.path, StandardOpenOption.READ);
                         FileChannel out = FileChannel.open(copy, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                        long copied = 0;
                        while (copied < chunk.size) {
                            copied += in.transferTo(copied, chunk.size - copied, out);
                        }
                        out.force(true);
                    }
                } else {
                    linkOrCopy(chunk.path, copy);
                }
            }
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot back up the log in " + dir + " to " + target, e);
        }
    }

    private static void linkOrCopy(Path source, Path target) throws IOException {
        try {
            Files.createLink(target, source);
        } catch (UnsupportedOperationException | IOException e) {
            Files.copy(source, target);
        }
    }

    /** Closes the log and deletes its files. */
    public void delete() {
        closeQuietly();
        try {
            if (Files.exists(dir)) {
                try (DirectoryStream<Path> files = Files.newDirectoryStream(dir)) {
                    for (Path file : files) {
                        Files.delete(file);
                    }
                }
                Files.delete(dir);
            }
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot delete the log in " + dir, e);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            if (activeChannel != null) {
                activeChannel.force(false);
                activeChannel.close();
            }
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot close the log in " + dir, e);
        }
    }

    private void closeQuietly() {
        try {
            close();
        } catch (RuntimeException ignored) {
            // closing after a failure
        }
    }

    private boolean isLive(ChunkReader reader) {
        Map<KeyBytes, Location> entries = index.get(reader.structureId);
        Location location = entries == null ? null : entries.get(new KeyBytes(reader.key));
        return location != null && location.seq == reader.seq;
    }

    private void release(Location location) {
        Chunk chunk = chunks.get(location.chunkId);
        if (chunk != null) {
            chunk.live -= location.size;
        }
    }

    private Location append(byte type, int structureId, byte[] key, byte[] value) {
        if (closed) {
            throw new PersistenceStoreException("The log in " + dir + " is closed");
        }
        int size = ChunkFormat.recordSize(key.length, value.length);
        if (buffer.capacity() < size) {
            buffer = ByteBuffer.allocate(Math.max(size, buffer.capacity() * 2));
        }
        buffer.clear();
        long seq = nextSeq++;
        ChunkFormat.writeRecord(buffer, type, seq, structureId, key, value);
        buffer.flip();
        try {
            while (buffer.hasRemaining()) {
                activeChannel.write(buffer, active.size + buffer.position());
            }
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot write to the log in " + dir, e);
        }
        active.size += size;
        unforced = true;
        return new Location(active.id, size, seq);
    }

    /**
     * When the active chunk is full, closes it, starts a new one and compacts if the closed chunks
     * hold enough garbage. Called once the index reflects the last record.
     */
    private void rollIfFull() {
        if (active.size < settings.maxChunkSize()) {
            return;
        }
        try {
            activeChannel.force(false);
            activeChannel.close();
            unforced = false;
            createActive(active.id + 1);
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot start a new chunk in " + dir, e);
        }
        maybeCompact();
    }

    private void maybeCompact() {
        long total = 0;
        long live = 0;
        for (Chunk chunk : chunks.headMap(active.id).values()) {
            total += chunk.size - HEADER_SIZE;
            live += chunk.live;
        }
        long garbage = total - live;
        if (garbage >= settings.maxChunkSize() && garbage >= total * settings.compactionGarbageRatio()) {
            compact();
        }
    }

    /**
     * Writes the live records of the closed chunks to one chunk that supersedes them (see the
     * class description).
     */
    void compact() {
        List<Chunk> closedChunks = new ArrayList<>(chunks.headMap(active.id).values());
        if (closedChunks.isEmpty()) {
            return;
        }
        long targetId = closedChunks.get(closedChunks.size() - 1).id;
        long coversFrom = closedChunks.get(0).id;
        Path temp = dir.resolve(ChunkFormat.idString(targetId) + ChunkFormat.TEMP_SUFFIX);
        Path target = dir.resolve(ChunkFormat.fileName(targetId));
        List<Moved> moved = new ArrayList<>();
        long size;
        try {
            size = writeLiveRecords(closedChunks, temp, targetId, coversFrom, moved);
        } catch (IOException e) {
            deleteQuietly(temp);
            throw new PersistenceStoreException("Cannot compact the log in " + dir, e);
        }
        try {
            Files.move(temp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            forceDirectory();
            for (Chunk chunk : closedChunks) {
                if (chunk.id != targetId) {
                    Files.delete(chunk.path);
                }
            }
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot compact the log in " + dir, e);
        }
        long live = 0;
        for (Chunk chunk : closedChunks) {
            chunks.remove(chunk.id);
        }
        for (Moved entry : moved) {
            index.get(entry.structureId).put(entry.key, entry.location);
            live += entry.location.size;
        }
        Chunk compacted = new Chunk(targetId, target);
        compacted.size = size;
        compacted.live = live;
        chunks.put(targetId, compacted);
    }

    /** @return the size of the written chunk */
    private long writeLiveRecords(List<Chunk> from, Path temp, long targetId, long coversFrom, List<Moved> moved)
            throws IOException {
        try (FileChannel out = FileChannel.open(temp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
            ChunkFormat.writeHeader(header, targetId, coversFrom);
            long size = writeFully(out, header.flip(), 0);
            for (Chunk chunk : from) {
                try (ChunkReader reader = new ChunkReader(chunk.path, true)) {
                    reader.readHeader();
                    while (reader.next()) {
                        if (reader.type == PUT && isLive(reader)) {
                            ByteBuffer record = ByteBuffer.allocate(reader.size);
                            ChunkFormat.writeRecord(record, PUT, reader.seq, reader.structureId, reader.key, reader.value);
                            size = writeFully(out, record.flip(), size);
                            moved.add(new Moved(reader.structureId, new KeyBytes(reader.key),
                                    new Location(targetId, reader.size, reader.seq)));
                        }
                    }
                }
            }
            out.force(true);
            return size;
        }
    }

    private static long writeFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        long at = position;
        while (buffer.hasRemaining()) {
            at += channel.write(buffer, at);
        }
        return at;
    }

    /** Reads the chunks: removes superseded and temporary files, truncates a torn end, builds the index. */
    private void load() throws IOException {
        TreeMap<Long, Path> files = new TreeMap<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path file : stream) {
                String name = file.getFileName().toString();
                if (name.endsWith(ChunkFormat.TEMP_SUFFIX)) {
                    Files.delete(file);
                } else if (name.endsWith(ChunkFormat.SUFFIX)) {
                    files.put(ChunkFormat.parseId(name), file);
                }
            }
        }
        // read the headers, dropping the chunks a compacted chunk supersedes
        TreeMap<Long, Long> coversFrom = new TreeMap<>();
        for (Map.Entry<Long, Path> file : files.entrySet()) {
            try (ChunkReader reader = new ChunkReader(file.getValue(), false)) {
                if (!reader.readHeader()) {
                    if (file.getKey().equals(files.lastKey())) {
                        // created, but its header was not written completely
                        Files.delete(file.getValue());
                        continue;
                    }
                    throw new PersistenceStoreException("Chunk " + file.getValue() + " has an invalid header");
                }
                coversFrom.put(file.getKey(), reader.coversFrom);
            }
        }
        for (Map.Entry<Long, Long> entry : coversFrom.descendingMap().entrySet().stream()
                .map(e -> Map.entry(e.getKey(), e.getValue())).toList()) {
            if (!coversFrom.containsKey(entry.getKey())) {
                continue;
            }
            for (Long superseded : List.copyOf(coversFrom.subMap(entry.getValue(), entry.getKey()).keySet())) {
                Files.delete(files.get(superseded));
                coversFrom.remove(superseded);
            }
        }
        for (Long id : coversFrom.keySet()) {
            Chunk chunk = new Chunk(id, files.get(id));
            chunks.put(id, chunk);
            readIndex(chunk, id.equals(coversFrom.lastKey()));
        }
    }

    private void readIndex(Chunk chunk, boolean last) throws IOException {
        long end;
        try (ChunkReader reader = new ChunkReader(chunk.path, false)) {
            reader.readHeader();
            while (reader.next()) {
                nextSeq = Math.max(nextSeq, reader.seq + 1);
                index(chunk, reader);
            }
            end = reader.invalidAt >= 0 ? reader.invalidAt : reader.offset;
            if (reader.invalidAt >= 0 && !last) {
                throw new PersistenceStoreException("Chunk " + chunk.path + " is corrupted at offset " + reader.invalidAt);
            }
        }
        chunk.size = end;
        if (last && Files.size(chunk.path) > end) {
            // a torn write at the end of the log
            try (FileChannel channel = FileChannel.open(chunk.path, StandardOpenOption.WRITE)) {
                channel.truncate(end);
                channel.force(true);
            }
        }
    }

    private void index(Chunk chunk, ChunkReader reader) {
        switch (reader.type) {
            case PUT -> {
                chunk.live += reader.size;
                Location previous = index.computeIfAbsent(reader.structureId, id -> new HashMap<>())
                        .put(new KeyBytes(reader.key), new Location(chunk.id, reader.size, reader.seq));
                if (previous != null) {
                    release(previous);
                }
            }
            case REMOVE -> {
                Map<KeyBytes, Location> entries = index.get(reader.structureId);
                Location previous = entries == null ? null : entries.remove(new KeyBytes(reader.key));
                if (previous != null) {
                    release(previous);
                }
            }
            case CLEAR -> {
                Map<KeyBytes, Location> entries = index.remove(reader.structureId);
                if (entries != null) {
                    entries.values().forEach(this::release);
                }
            }
            default -> throw new PersistenceStoreException("Chunk " + chunk.path + " has a record of unknown type "
                    + reader.type + " at offset " + reader.offset);
        }
    }

    private void openActive() throws IOException {
        if (chunks.isEmpty()) {
            createActive(1);
            return;
        }
        Chunk last = chunks.lastEntry().getValue();
        if (last.size >= settings.maxChunkSize()) {
            createActive(last.id + 1);
        } else {
            active = last;
            activeChannel = FileChannel.open(last.path, StandardOpenOption.WRITE);
        }
    }

    private void createActive(long id) throws IOException {
        Path path = dir.resolve(ChunkFormat.fileName(id));
        FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        ChunkFormat.writeHeader(header, id, id);
        writeFully(channel, header.flip(), 0);
        channel.force(true);
        forceDirectory();
        Chunk chunk = new Chunk(id, path);
        chunk.size = HEADER_SIZE;
        chunks.put(id, chunk);
        active = chunk;
        activeChannel = channel;
    }

    private void forceDirectory() {
        try (FileChannel channel = FileChannel.open(dir, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException e) {
            // not supported by every platform; the files themselves are forced
        }
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // best effort after a failure
        }
    }

    /** A chunk file: its size and the bytes of its live records. */
    private static final class Chunk {
        final long id;
        final Path path;
        long size;
        long live;

        Chunk(long id, Path path) {
            this.id = id;
            this.path = path;
        }
    }

    /** Where the live record of a key is. */
    private record Location(long chunkId, int size, long seq) {
    }

    /** A live record copied by compaction. */
    private record Moved(int structureId, KeyBytes key, Location location) {
    }
}
