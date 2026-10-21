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

package uk.org.hecaton.tiered;

import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.impl.HeapData;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * The disk tier of one partition of a map: values appended to a file, read back by position. A
 * value that is removed or moved back to memory becomes garbage; the owner rewrites the live
 * values to a new file when there is more garbage than live data.
 * <p>
 * Written by the partition's thread only; reads ({@link #read}) are positional and safe from
 * any thread.
 */
final class DiskTier {

    private final Path file;
    private FileChannel channel;
    private long end;
    private long live;
    private long garbage;

    DiskTier(Path file) {
        this.file = file;
    }

    Path file() {
        return file;
    }

    /** Appends a value; returns its offset. */
    long append(Data value) {
        byte[] bytes = bytes(value);
        try {
            if (channel == null) {
                Files.createDirectories(file.getParent());
                channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.READ, StandardOpenOption.WRITE);
            }
            long offset = end;
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            long position = offset;
            while (buffer.hasRemaining()) {
                position += channel.write(buffer, position);
            }
            end += bytes.length;
            live += bytes.length;
            return offset;
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot write to the disk tier " + file, e);
        }
    }

    Data read(long offset, int length) {
        byte[] bytes = new byte[length];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        try {
            long position = offset;
            while (buffer.hasRemaining()) {
                int read = channel.read(buffer, position);
                if (read < 0) {
                    throw new IOException("Unexpected end of file at " + position);
                }
                position += read;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read from the disk tier " + file, e);
        }
        return new HeapData(bytes);
    }

    /** A value of {@code length} bytes is not needed any more. */
    void free(int length) {
        live -= length;
        garbage += length;
    }

    long live() {
        return live;
    }

    long garbage() {
        return garbage;
    }

    /** Closes and deletes the file. */
    void delete() {
        try {
            if (channel != null) {
                channel.close();
                channel = null;
            }
            Files.deleteIfExists(file);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot delete the disk tier " + file, e);
        } finally {
            end = 0;
            live = 0;
            garbage = 0;
        }
    }

    static int length(Data value) {
        return bytes(value).length;
    }

    private static byte[] bytes(Data value) {
        byte[] bytes = value.toByteArray();
        return bytes == null ? new byte[0] : bytes;
    }
}
