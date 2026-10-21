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

import uk.org.hecaton.internal.nio.BufferObjectDataInput;
import uk.org.hecaton.internal.nio.BufferObjectDataOutput;
import uk.org.hecaton.internal.serialization.InternalSerializationService;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;

/**
 * Small files of CP persistence, written atomically: the content and its CRC-32C go to a temporary
 * file, which is forced to disk and renamed over the target.
 */
final class CPFiles {

    private static final int CRC_SIZE = 4;

    private CPFiles() {
    }

    /** Writes {@code content} to {@code file}, replacing it atomically. */
    static void write(Path file, byte[] content) throws IOException {
        Path temp = file.resolveSibling(file.getFileName() + ".tmp");
        ByteBuffer buffer = ByteBuffer.allocate(content.length + CRC_SIZE);
        buffer.put(content).putInt(crc(content, 0, content.length)).flip();
        try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            writeFully(channel, buffer);
            channel.force(true);
        }
        Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        forceDirectory(file.getParent());
    }

    /**
     * @return the content of {@code file}, or null if it does not exist
     * @throws IOException if it cannot be read or its checksum does not match
     */
    static byte[] read(Path file) throws IOException {
        if (!Files.exists(file)) {
            return null;
        }
        byte[] bytes = Files.readAllBytes(file);
        int length = bytes.length - CRC_SIZE;
        if (length < 0 || ByteBuffer.wrap(bytes, length, CRC_SIZE).getInt() != crc(bytes, 0, length)) {
            throw new IOException(file + " is corrupted");
        }
        byte[] content = new byte[length];
        System.arraycopy(bytes, 0, content, 0, length);
        return content;
    }

    /** Serializes objects with the member's serialization service. */
    static byte[] serialize(InternalSerializationService serializationService, Object... objects) throws IOException {
        BufferObjectDataOutput out = serializationService.createObjectDataOutput();
        for (Object object : objects) {
            out.writeObject(object);
        }
        return out.toByteArray();
    }

    static BufferObjectDataInput input(InternalSerializationService serializationService, byte[] bytes) {
        return serializationService.createObjectDataInput(bytes);
    }

    static int crc(byte[] bytes, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }

    static void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    /** Forces the directory entry changes (creations, renames) to disk, where the platform supports it. */
    static void forceDirectory(Path dir) {
        try (FileChannel channel = FileChannel.open(dir, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException e) {
            // not supported by every platform; the files themselves are forced
            return;
        }
    }

    /** Deletes a directory and its content, if it exists. */
    static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        if (Files.isDirectory(path)) {
            try (DirectoryStream<Path> children = Files.newDirectoryStream(path)) {
                for (Path child : children) {
                    deleteRecursively(child);
                }
            }
        }
        Files.delete(path);
    }
}
