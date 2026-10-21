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

package uk.org.hecaton.cp.internal.persistence;

import uk.org.hecaton.cp.internal.raft.impl.RaftEndpoint;
import uk.org.hecaton.cp.internal.raft.impl.log.LogEntry;
import uk.org.hecaton.cp.internal.raft.impl.log.SnapshotEntry;
import uk.org.hecaton.cp.internal.raft.impl.persistence.RaftStateStore;
import uk.org.hecaton.internal.serialization.InternalSerializationService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

/**
 * The persisted state of the local Raft node of one CP group, in its own directory:
 * <ul>
 *     <li>{@code members}: the local endpoint and the initial members of the group;</li>
 *     <li>{@code term}: the current term and the endpoint voted for in it;</li>
 *     <li>{@code log-<generation>}: the Raft log, an append-only file of records (log entries,
 *     truncations). Each snapshot starts a new generation, whose file begins with the snapshot
 *     followed by the persisted entries after it; once it is on disk, the older files are deleted.</li>
 * </ul>
 * A record is the length of its payload, a CRC-32C of its type and payload, its type and its
 * payload. A torn record at the end of the log (a crash during a write) ends the log.
 * Called by the Raft node's thread only.
 */
final class FileRaftStateStore implements RaftStateStore {

    static final String LOG_FILE_PREFIX = "log-";
    static final String TERM_FILE = "term";
    static final String MEMBERS_FILE = "members";

    static final byte ENTRY = 1;
    static final byte SNAPSHOT = 2;
    static final byte TRUNCATE = 3;
    /** Payload length and CRC. */
    static final int RECORD_HEADER_SIZE = Integer.BYTES * 2;

    private final Path dir;
    private final InternalSerializationService serializationService;
    /** The persisted entries after the snapshot, serialized, to copy into the next generation. */
    private final Deque<PersistedEntry> entries = new ArrayDeque<>();
    private final RaftLogFile restored;
    private long generation;
    private FileChannel log;

    /**
     * @param restored the log read at the member's start, to append to, or null for a new group
     */
    FileRaftStateStore(Path dir, InternalSerializationService serializationService, @Nullable RaftLogFile restored) {
        this.dir = dir;
        this.serializationService = serializationService;
        this.restored = restored;
    }

    @Override
    public void open() throws IOException {
        Files.createDirectories(dir);
        if (restored == null) {
            log = openLog(0, true);
            return;
        }
        generation = restored.generation();
        entries.addAll(restored.entries());
        log = openLog(generation, false);
        // drop a torn record at the end
        log.truncate(restored.validLength());
        log.position(restored.validLength());
        deleteOtherGenerations();
    }

    @Override
    public void persistInitialMembers(@Nonnull RaftEndpoint localMember, @Nonnull Collection<RaftEndpoint> initialMembers)
            throws IOException {
        if (restored != null) {
            return;
        }
        List<Object> objects = new ArrayList<>(initialMembers.size() + 2);
        objects.add(localMember);
        objects.add(initialMembers.size());
        objects.addAll(initialMembers);
        CPFiles.write(dir.resolve(MEMBERS_FILE), CPFiles.serialize(serializationService, objects.toArray()));
    }

    @Override
    public void persistTerm(int term, @Nullable RaftEndpoint votedFor) throws IOException {
        CPFiles.write(dir.resolve(TERM_FILE), CPFiles.serialize(serializationService, term, votedFor));
    }

    @Override
    public void persistEntry(@Nonnull LogEntry entry) throws IOException {
        byte[] payload = CPFiles.serialize(serializationService, entry);
        append(log, ENTRY, payload);
        entries.addLast(new PersistedEntry(entry.index(), payload));
    }

    @Override
    public void persistSnapshot(@Nonnull SnapshotEntry snapshot) throws IOException {
        long next = generation + 1;
        FileChannel nextLog = openLog(next, true);
        try {
            append(nextLog, SNAPSHOT, CPFiles.serialize(serializationService, snapshot));
            entries.removeIf(entry -> entry.index() <= snapshot.index());
            for (PersistedEntry entry : entries) {
                append(nextLog, ENTRY, entry.payload());
            }
            nextLog.force(false);
            CPFiles.forceDirectory(dir);
        } catch (IOException e) {
            nextLog.close();
            throw e;
        }
        log.close();
        log = nextLog;
        generation = next;
        deleteOtherGenerations();
    }

    @Override
    public void deleteEntriesFrom(long startIndexInclusive) throws IOException {
        append(log, TRUNCATE, ByteBuffer.allocate(Long.BYTES).putLong(startIndexInclusive).array());
        while (!entries.isEmpty() && entries.peekLast().index() >= startIndexInclusive) {
            entries.removeLast();
        }
    }

    @Override
    public void flushLogs() throws IOException {
        log.force(false);
    }

    @Override
    public void persistSnapshotChunk(Object snapshotChunk) {
        // snapshots are not chunked: persistSnapshot writes them whole
    }

    @Override
    public void deleteSnapshotChunks(long snapshotIndex) {
        // snapshots are not chunked
    }

    @Override
    public boolean isChunkingSupportedVersion() {
        return false;
    }

    @Override
    public void close() throws IOException {
        if (log != null) {
            log.close();
        }
    }

    private FileChannel openLog(long gen, boolean truncate) throws IOException {
        Path file = dir.resolve(logFileName(gen));
        return truncate
                ? FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING)
                : FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    }

    private void deleteOtherGenerations() throws IOException {
        String current = logFileName(generation);
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, LOG_FILE_PREFIX + "*")) {
            for (Path file : files) {
                if (!file.getFileName().toString().equals(current)) {
                    Files.delete(file);
                }
            }
        }
    }

    static String logFileName(long generation) {
        return LOG_FILE_PREFIX + Long.toHexString(generation);
    }

    private static void append(FileChannel channel, byte type, byte[] payload) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(RECORD_HEADER_SIZE + 1 + payload.length);
        buffer.putInt(payload.length);
        buffer.putInt(0);
        buffer.put(type).put(payload);
        byte[] array = buffer.array();
        buffer.putInt(Integer.BYTES, CPFiles.crc(array, RECORD_HEADER_SIZE, 1 + payload.length));
        buffer.flip();
        CPFiles.writeFully(channel, buffer);
    }

    /** A persisted log entry: its index and serialized form. */
    record PersistedEntry(long index, byte[] payload) {
    }

    /** Removes the entries from {@code index} on (a truncation record). */
    static void truncate(List<PersistedEntry> entries, long index) {
        Iterator<PersistedEntry> iterator = entries.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().index() >= index) {
                iterator.remove();
            }
        }
    }
}
