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
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static uk.org.hecaton.persistence.internal.store.ChunkFormat.BODY_FIXED_SIZE;
import static uk.org.hecaton.persistence.internal.store.ChunkFormat.HEADER_SIZE;
import static uk.org.hecaton.persistence.internal.store.ChunkFormat.RECORD_HEADER_SIZE;

/** Reads a chunk file from start to end. */
final class ChunkReader implements Closeable {

    private static final int BUFFER_SIZE = 1 << 16;

    long chunkId;
    long coversFrom;

    // the current record
    long offset;
    int size;
    byte type;
    long seq;
    int structureId;
    byte[] key;
    /** Null unless values are read. */
    byte[] value;

    /** Set when the chunk does not end with a complete, intact record: the offset of the bad bytes. */
    long invalidAt = -1;

    private final FileChannel channel;
    private final boolean readValues;
    private ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE).flip();
    /** File position of the end of the buffered bytes. */
    private long filePosition;

    ChunkReader(Path path, boolean readValues) throws IOException {
        this.channel = FileChannel.open(path, StandardOpenOption.READ);
        this.readValues = readValues;
    }

    /** @return whether the chunk has a valid header (it was read) */
    boolean readHeader() throws IOException {
        if (!ensure(HEADER_SIZE)) {
            invalidAt = 0;
            return false;
        }
        int start = buffer.position();
        int crc = ChunkFormat.crc(buffer, start, HEADER_SIZE - ChunkFormat.CRC_SIZE);
        int magic = buffer.getInt();
        int format = buffer.getInt();
        chunkId = buffer.getLong();
        coversFrom = buffer.getLong();
        if (magic != ChunkFormat.MAGIC || format != ChunkFormat.FORMAT || buffer.getInt() != crc) {
            invalidAt = 0;
            return false;
        }
        return true;
    }

    /** @return whether a record was read; false at the end or at an invalid record ({@link #invalidAt}) */
    boolean next() throws IOException {
        offset = filePosition - buffer.remaining();
        if (!ensure(RECORD_HEADER_SIZE)) {
            if (buffer.hasRemaining()) {
                invalidAt = offset;
            }
            return false;
        }
        int crc = buffer.getInt();
        int bodyLength = buffer.getInt();
        if (!readBody(crc, bodyLength)) {
            invalidAt = offset;
            return false;
        }
        size = RECORD_HEADER_SIZE + bodyLength;
        return true;
    }

    private boolean readBody(int crc, int bodyLength) throws IOException {
        if (bodyLength < BODY_FIXED_SIZE || bodyLength > channel.size() - offset - RECORD_HEADER_SIZE
                || !ensure(bodyLength) || ChunkFormat.crc(buffer, buffer.position(), bodyLength) != crc) {
            return false;
        }
        type = buffer.get();
        seq = buffer.getLong();
        structureId = buffer.getInt();
        int keyLength = buffer.getInt();
        if (keyLength < 0 || keyLength > bodyLength - BODY_FIXED_SIZE) {
            return false;
        }
        key = new byte[keyLength];
        buffer.get(key);
        int valueLength = buffer.getInt();
        if (valueLength != bodyLength - BODY_FIXED_SIZE - keyLength) {
            return false;
        }
        if (readValues) {
            value = new byte[valueLength];
            buffer.get(value);
        } else {
            buffer.position(buffer.position() + valueLength);
        }
        return true;
    }

    /** Makes {@code n} bytes available in the buffer, unless the file ends first. */
    private boolean ensure(int n) throws IOException {
        if (buffer.remaining() >= n) {
            return true;
        }
        if (buffer.capacity() < n) {
            ByteBuffer larger = ByteBuffer.allocate(Math.max(n, buffer.capacity() * 2));
            larger.put(buffer);
            buffer = larger;
        } else {
            buffer.compact();
        }
        while (buffer.position() < n) {
            int read = channel.read(buffer, filePosition);
            if (read < 0) {
                break;
            }
            filePosition += read;
        }
        buffer.flip();
        return buffer.remaining() >= n;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
