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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;

/**
 * Small files written atomically: the content and its CRC-32C go to a temporary file, which is
 * forced to disk and renamed over the target.
 */
public final class AtomicFiles {

    private static final int CRC_SIZE = 4;

    private AtomicFiles() {
    }

    /** Writes {@code content} to {@code file}, replacing it atomically. */
    public static void write(Path file, byte[] content) {
        Path temp = file.resolveSibling(file.getFileName() + ".tmp");
        try {
            ByteBuffer buffer = ByteBuffer.allocate(content.length + CRC_SIZE);
            buffer.put(content).putInt(crc(content));
            try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                buffer.flip();
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            forceDirectory(file.getParent());
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot write " + file, e);
        }
    }

    /** Writes {@code content} as UTF-8. */
    public static void writeString(Path file, String content) {
        write(file, content.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * @return the content of {@code file}, or null if it does not exist
     * @throws PersistenceStoreException if its checksum does not match
     */
    public static byte[] read(Path file) {
        if (!Files.exists(file)) {
            return null;
        }
        try {
            byte[] bytes = Files.readAllBytes(file);
            if (bytes.length < CRC_SIZE) {
                throw new PersistenceStoreException(file + " is corrupted");
            }
            byte[] content = new byte[bytes.length - CRC_SIZE];
            System.arraycopy(bytes, 0, content, 0, content.length);
            if (ByteBuffer.wrap(bytes, content.length, CRC_SIZE).getInt() != crc(content)) {
                throw new PersistenceStoreException(file + " is corrupted");
            }
            return content;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** @return the content of {@code file} as UTF-8, or null if it does not exist */
    public static String readString(Path file) {
        byte[] content = read(file);
        return content == null ? null : new String(content, StandardCharsets.UTF_8);
    }

    /** Forces the directory entry changes (creations, renames) to disk, where the platform supports it. */
    public static void forceDirectory(Path dir) {
        try (FileChannel channel = FileChannel.open(dir, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException e) {
            // not supported by every platform; the files themselves are forced
        }
    }

    private static int crc(byte[] content) {
        CRC32C crc = new CRC32C();
        crc.update(content);
        return (int) crc.getValue();
    }
}
