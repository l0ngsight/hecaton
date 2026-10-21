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
package uk.org.hecaton.persistence.internal;

import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.persistence.internal.store.AtomicFiles;
import uk.org.hecaton.persistence.internal.store.PersistenceStoreException;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * The directory of one member under the base directory, locked while the member runs, so members
 * sharing a base directory each get their own.
 */
final class MemberDirectory implements AutoCloseable {

    private static final String LOCK = "lock";
    private static final String MEMBER = "member";

    private final Path dir;
    private final FileChannel lockChannel;
    private final FileLock lock;

    private MemberDirectory(Path dir, FileChannel lockChannel, FileLock lock) {
        this.dir = dir;
        this.lockChannel = lockChannel;
        this.lock = lock;
    }

    /**
     * Claims the first directory of an earlier member under {@code baseDir} that no running member
     * holds, or creates a new one.
     */
    static MemberDirectory claim(Path baseDir) {
        try {
            Files.createDirectories(baseDir);
            List<Path> candidates = new ArrayList<>();
            try (DirectoryStream<Path> dirs = Files.newDirectoryStream(baseDir, Files::isDirectory)) {
                dirs.forEach(candidates::add);
            }
            candidates.sort(null);
            for (Path candidate : candidates) {
                if (Files.exists(candidate.resolve(MEMBER))) {
                    MemberDirectory claimed = tryLock(candidate);
                    if (claimed != null) {
                        return claimed;
                    }
                }
            }
            Path dir = baseDir.resolve(UuidUtil.newUnsecureUUID().toString());
            Files.createDirectories(dir);
            MemberDirectory claimed = tryLock(dir);
            if (claimed == null) {
                throw new PersistenceStoreException("Cannot lock the new directory " + dir);
            }
            return claimed;
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot claim a member directory in " + baseDir, e);
        }
    }

    private static MemberDirectory tryLock(Path dir) throws IOException {
        FileChannel channel = FileChannel.open(dir.resolve(LOCK), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock lock;
        try {
            lock = channel.tryLock();
        } catch (OverlappingFileLockException e) {
            // held by another member in this JVM
            lock = null;
        }
        if (lock == null) {
            channel.close();
            return null;
        }
        return new MemberDirectory(dir, channel, lock);
    }

    Path path() {
        return dir;
    }

    /** @return the UUID of the member that used the directory, or null if the directory is new */
    UUID memberUuid() {
        String uuid = AtomicFiles.readString(dir.resolve(MEMBER));
        return uuid == null ? null : UUID.fromString(uuid.trim());
    }

    void writeMemberUuid(UUID uuid) {
        AtomicFiles.writeString(dir.resolve(MEMBER), uuid.toString());
    }

    @Override
    public void close() {
        try {
            lock.release();
            lockChannel.close();
        } catch (IOException e) {
            throw new PersistenceStoreException("Cannot release " + dir, e);
        }
    }
}
