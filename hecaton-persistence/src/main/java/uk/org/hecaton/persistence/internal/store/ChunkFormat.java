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

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

/**
 * The layout of chunk files.
 * <p>
 * Header: magic, format version, chunk id, id of the oldest chunk the chunk supersedes, CRC-32C
 * of the preceding fields. Record: CRC-32C of the body, length of the body, body; body: type,
 * sequence number, structure id, key length, key, value length, value.
 */
final class ChunkFormat {

    static final int MAGIC = 0x48434c47;
    static final int FORMAT = 1;
    static final int CRC_SIZE = 4;
    static final int HEADER_SIZE = 4 + 4 + 8 + 8 + CRC_SIZE;
    static final String SUFFIX = ".chunk";
    static final String TEMP_SUFFIX = ".chunk.tmp";

    static final byte PUT = 1;
    static final byte REMOVE = 2;
    static final byte CLEAR = 3;

    /** CRC and body length. */
    static final int RECORD_HEADER_SIZE = 4 + 4;
    /** Body without key and value: type, seq, structure, key length, value length. */
    static final int BODY_FIXED_SIZE = 1 + 8 + 4 + 4 + 4;

    private static final int HEX = 16;

    private ChunkFormat() {
    }

    static String fileName(long chunkId) {
        return idString(chunkId) + SUFFIX;
    }

    static String idString(long chunkId) {
        return String.format("%016x", chunkId);
    }

    static long parseId(String fileName) {
        return Long.parseUnsignedLong(fileName.substring(0, fileName.length() - SUFFIX.length()), HEX);
    }

    static int recordSize(int keyLength, int valueLength) {
        return RECORD_HEADER_SIZE + BODY_FIXED_SIZE + keyLength + valueLength;
    }

    static void writeHeader(ByteBuffer buffer, long chunkId, long coversFrom) {
        int start = buffer.position();
        buffer.putInt(MAGIC).putInt(FORMAT).putLong(chunkId).putLong(coversFrom);
        buffer.putInt(crc(buffer, start, HEADER_SIZE - CRC_SIZE));
    }

    /** Writes a record at the buffer's position; the buffer must have room for it. */
    static void writeRecord(ByteBuffer buffer, byte type, long seq, int structureId, byte[] key, byte[] value) {
        int start = buffer.position();
        int bodyLength = BODY_FIXED_SIZE + key.length + value.length;
        buffer.position(start + RECORD_HEADER_SIZE);
        buffer.put(type).putLong(seq).putInt(structureId).putInt(key.length).put(key).putInt(value.length).put(value);
        int crc = crc(buffer, start + RECORD_HEADER_SIZE, bodyLength);
        buffer.putInt(start, crc).putInt(start + CRC_SIZE, bodyLength);
    }

    static int crc(ByteBuffer buffer, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(buffer.slice(offset, length));
        return (int) crc.getValue();
    }
}
