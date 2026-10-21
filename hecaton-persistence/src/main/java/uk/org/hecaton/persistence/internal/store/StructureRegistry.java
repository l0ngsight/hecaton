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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32C;

/**
 * The ids of the persisted structures (a map or cache name with its kind), kept in an append-only
 * file: entries are {@code [crc][length][id, kind, name]}; a torn entry at the end is truncated.
 * Thread-safe.
 */
public final class StructureRegistry implements AutoCloseable {

    private static final int ENTRY_HEADER = 8;
    private static final int ID_AND_KIND = Integer.BYTES + 1;

    private final Path file;
    private final Map<String, Integer> ids = new HashMap<>();
    private final Map<Integer, Structure> structures = new HashMap<>();
    private final FileChannel channel;
    private long size;

    public StructureRegistry(Path file) {
        this.file = file;
        try {
            channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            load();
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot open " + file, e);
        }
    }

    /** The id of the structure, assigned (and stored) if it has none yet. */
    public synchronized int idOf(byte kind, String name) {
        String key = kind + ":" + name;
        Integer id = ids.get(key);
        if (id != null) {
            return id;
        }
        int newId = structures.size() + 1;
        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        ByteBuffer body = ByteBuffer.allocate(ID_AND_KIND + nameBytes.length);
        body.putInt(newId).put(kind).put(nameBytes).flip();
        CRC32C crc = new CRC32C();
        crc.update(body.duplicate());
        ByteBuffer entry = ByteBuffer.allocate(ENTRY_HEADER + body.remaining());
        entry.putInt((int) crc.getValue()).putInt(body.remaining()).put(body).flip();
        try {
            while (entry.hasRemaining()) {
                size += channel.write(entry, size);
            }
            channel.force(false);
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot write " + file, e);
        }
        ids.put(key, newId);
        structures.put(newId, new Structure(newId, kind, name));
        return newId;
    }

    /** @return the structure with the id, or null */
    public synchronized Structure get(int id) {
        return structures.get(id);
    }

    @Override
    public synchronized void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot close " + file, e);
        }
    }

    private void load() throws IOException {
        long fileSize = channel.size();
        ByteBuffer bytes = ByteBuffer.allocate((int) fileSize);
        boolean more = true;
        while (bytes.hasRemaining() && more) {
            more = channel.read(bytes, bytes.position()) >= 0;
        }
        bytes.flip();
        while (bytes.remaining() >= ENTRY_HEADER) {
            int start = bytes.position();
            int crc = bytes.getInt();
            int length = bytes.getInt();
            if (length < ID_AND_KIND || length > bytes.remaining()) {
                bytes.position(start);
                break;
            }
            CRC32C actual = new CRC32C();
            actual.update(bytes.slice(bytes.position(), length));
            if ((int) actual.getValue() != crc) {
                bytes.position(start);
                break;
            }
            int id = bytes.getInt();
            byte kind = bytes.get();
            byte[] name = new byte[length - ID_AND_KIND];
            bytes.get(name);
            Structure structure = new Structure(id, kind, new String(name, StandardCharsets.UTF_8));
            structures.put(id, structure);
            ids.put(kind + ":" + structure.name(), id);
        }
        size = bytes.position();
        if (size < fileSize) {
            channel.truncate(size);
        }
    }

    /**
     * A persisted structure.
     *
     * @param id   its id in the logs
     * @param kind {@link #MAP} or {@link #CACHE}
     * @param name its name
     */
    public record Structure(int id, byte kind, String name) {
        /** An IMap. */
        public static final byte MAP = 1;
        /** An ICache. */
        public static final byte CACHE = 2;
    }
}
