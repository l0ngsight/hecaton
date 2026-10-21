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

import uk.org.hecaton.cp.internal.persistence.FileRaftStateStore.PersistedEntry;
import uk.org.hecaton.cp.internal.raft.impl.RaftEndpoint;
import uk.org.hecaton.cp.internal.raft.impl.log.LogEntry;
import uk.org.hecaton.cp.internal.raft.impl.log.SnapshotEntry;
import uk.org.hecaton.cp.internal.raft.impl.persistence.RaftStateLoader;
import uk.org.hecaton.cp.internal.raft.impl.persistence.RestoredRaftState;
import uk.org.hecaton.internal.nio.BufferObjectDataInput;
import uk.org.hecaton.internal.serialization.InternalSerializationService;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static uk.org.hecaton.cp.internal.persistence.FileRaftStateStore.ENTRY;
import static uk.org.hecaton.cp.internal.persistence.FileRaftStateStore.LOG_FILE_PREFIX;
import static uk.org.hecaton.cp.internal.persistence.FileRaftStateStore.MEMBERS_FILE;
import static uk.org.hecaton.cp.internal.persistence.FileRaftStateStore.RECORD_HEADER_SIZE;
import static uk.org.hecaton.cp.internal.persistence.FileRaftStateStore.SNAPSHOT;
import static uk.org.hecaton.cp.internal.persistence.FileRaftStateStore.TERM_FILE;
import static uk.org.hecaton.cp.internal.persistence.FileRaftStateStore.TRUNCATE;

/**
 * Reads the state that a {@link FileRaftStateStore} persisted in a CP group's directory. The log
 * read is the newest generation that is complete: a generation whose file does not start with its
 * snapshot was being written when the member stopped, and the one before it is used.
 */
final class FileRaftStateLoader implements RaftStateLoader {

    private static final int HEX = 16;

    private final Path dir;
    private final InternalSerializationService serializationService;
    private RaftLogFile logFile;

    FileRaftStateLoader(Path dir, InternalSerializationService serializationService) {
        this.dir = dir;
        this.serializationService = serializationService;
    }

    /** Whether the directory holds the state of a Raft node (it was created before the member stopped). */
    static boolean hasState(Path dir) {
        return Files.exists(dir.resolve(MEMBERS_FILE));
    }

    @Nonnull
    @Override
    public RestoredRaftState load() throws IOException {
        byte[] members = CPFiles.read(dir.resolve(MEMBERS_FILE));
        if (members == null) {
            throw new IOException("No Raft state in " + dir);
        }
        BufferObjectDataInput in = CPFiles.input(serializationService, members);
        RaftEndpoint localEndpoint = in.readObject();
        int count = in.readObject();
        Collection<RaftEndpoint> initialMembers = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            initialMembers.add(in.readObject());
        }

        int term = 0;
        RaftEndpoint votedFor = null;
        byte[] termBytes = CPFiles.read(dir.resolve(TERM_FILE));
        if (termBytes != null) {
            BufferObjectDataInput termIn = CPFiles.input(serializationService, termBytes);
            term = termIn.readObject();
            votedFor = termIn.readObject();
        }

        LogContent content = readLog();
        LogEntry[] entries = new LogEntry[content.entries.size()];
        for (int i = 0; i < entries.length; i++) {
            entries[i] = CPFiles.input(serializationService, content.entries.get(i).payload()).readObject();
        }
        logFile = new RaftLogFile(content.generation, content.validLength, content.entries);
        return new RestoredRaftState(localEndpoint, initialMembers, term, votedFor, content.snapshot, entries);
    }

    /** The log file that {@link #load()} read. */
    RaftLogFile logFile() {
        return logFile;
    }

    private LogContent readLog() throws IOException {
        List<Long> generations = new ArrayList<>();
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir, LOG_FILE_PREFIX + "*")) {
            for (Path file : files) {
                generations.add(Long.parseLong(file.getFileName().toString().substring(LOG_FILE_PREFIX.length()), HEX));
            }
        }
        generations.sort(null);
        for (int i = generations.size() - 1; i >= 0; i--) {
            LogContent content = readLog(generations.get(i));
            if (content != null) {
                return content;
            }
        }
        return new LogContent(0, 0, null, new ArrayList<>());
    }

    /** @return the content of a generation, or null if it is incomplete */
    private LogContent readLog(long generation) throws IOException {
        byte[] bytes = Files.readAllBytes(dir.resolve(FileRaftStateStore.logFileName(generation)));
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        SnapshotEntry snapshot = null;
        List<PersistedEntry> entries = new ArrayList<>();
        long validLength = 0;
        while (buffer.remaining() >= RECORD_HEADER_SIZE + 1) {
            int start = buffer.position();
            int length = buffer.getInt();
            int crc = buffer.getInt();
            if (length < 0 || buffer.remaining() < 1 + length
                    || crc != CPFiles.crc(bytes, start + RECORD_HEADER_SIZE, 1 + length)) {
                break;
            }
            byte type = buffer.get();
            byte[] payload = new byte[length];
            buffer.get(payload);
            if (type == SNAPSHOT && validLength == 0) {
                snapshot = CPFiles.input(serializationService, payload).readObject();
            } else if (type == ENTRY) {
                LogEntry entry = CPFiles.input(serializationService, payload).readObject();
                entries.add(new PersistedEntry(entry.index(), payload));
            } else if (type == TRUNCATE) {
                FileRaftStateStore.truncate(entries, ByteBuffer.wrap(payload).getLong());
            } else {
                throw new IOException("Unexpected record of type " + type + " in the Raft log of " + dir);
            }
            validLength = buffer.position();
        }
        if (generation > 0 && snapshot == null) {
            return null;
        }
        return new LogContent(generation, validLength, snapshot, entries);
    }

    private record LogContent(long generation, long validLength, SnapshotEntry snapshot, List<PersistedEntry> entries) {
    }
}
