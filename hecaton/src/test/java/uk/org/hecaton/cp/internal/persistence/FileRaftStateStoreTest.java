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

import uk.org.hecaton.cp.internal.RaftEndpointImpl;
import uk.org.hecaton.cp.internal.raft.impl.RaftEndpoint;
import uk.org.hecaton.cp.internal.raft.impl.log.LogEntry;
import uk.org.hecaton.cp.internal.raft.impl.log.SnapshotEntry;
import uk.org.hecaton.cp.internal.raft.impl.persistence.RestoredRaftState;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Raft state store of CP Subsystem persistence (F9): what it persists is what a restart restores.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FileRaftStateStoreTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
    private final RaftEndpoint local = new RaftEndpointImpl(UUID.randomUUID());
    private final List<RaftEndpoint> members = List.of(local, new RaftEndpointImpl(UUID.randomUUID()),
            new RaftEndpointImpl(UUID.randomUUID()));

    @Test
    public void restoresMembersTermAndEntries() throws IOException {
        Path dir = folder.newFolder().toPath();
        FileRaftStateStore store = newStore(dir, null);
        store.persistTerm(3, members.get(1));
        for (int i = 1; i <= 5; i++) {
            store.persistEntry(entry(3, i));
        }
        store.flushLogs();
        store.close();

        RestoredRaftState state = load(dir);
        assertThat(state.localEndpoint()).isEqualTo(local);
        assertThat(state.initialMembers()).containsExactlyElementsOf(members);
        assertThat(state.term()).isEqualTo(3);
        assertThat(state.votedFor()).isEqualTo(members.get(1));
        assertThat(state.snapshot()).isNull();
        assertThat(indices(state)).containsExactly(1L, 2L, 3L, 4L, 5L);
        assertThat(state.entries()[4].operation()).isEqualTo("op-5");
    }

    @Test
    public void truncatedEntriesAreNotRestored() throws IOException {
        Path dir = folder.newFolder().toPath();
        FileRaftStateStore store = newStore(dir, null);
        for (int i = 1; i <= 5; i++) {
            store.persistEntry(entry(1, i));
        }
        store.deleteEntriesFrom(4);
        store.persistEntry(entry(2, 4));
        store.close();

        RestoredRaftState state = load(dir);
        assertThat(indices(state)).containsExactly(1L, 2L, 3L, 4L);
        assertThat(state.entries()[3].term()).isEqualTo(2);
    }

    @Test
    public void snapshotStartsANewGenerationWithTheLaterEntries() throws IOException {
        Path dir = folder.newFolder().toPath();
        FileRaftStateStore store = newStore(dir, null);
        for (int i = 1; i <= 12; i++) {
            store.persistEntry(entry(1, i));
        }
        store.persistSnapshot(new SnapshotEntry(1, 10, "snapshot-10", 0, members));
        store.persistEntry(entry(1, 13));
        store.close();

        assertThat(Files.exists(dir.resolve(FileRaftStateStore.logFileName(0)))).isFalse();
        RestoredRaftState state = load(dir);
        assertThat(state.snapshot().index()).isEqualTo(10);
        assertThat(state.snapshot().operation()).isEqualTo("snapshot-10");
        assertThat(indices(state)).containsExactly(11L, 12L, 13L);
    }

    @Test
    public void restoredStoreKeepsAppending() throws IOException {
        Path dir = folder.newFolder().toPath();
        FileRaftStateStore store = newStore(dir, null);
        for (int i = 1; i <= 3; i++) {
            store.persistEntry(entry(1, i));
        }
        store.close();

        FileRaftStateLoader loader = new FileRaftStateLoader(dir, serializationService);
        loader.load();
        store = newStore(dir, loader.logFile());
        store.persistEntry(entry(1, 4));
        store.persistSnapshot(new SnapshotEntry(1, 2, "snapshot-2", 0, members));
        store.persistEntry(entry(1, 5));
        store.close();

        RestoredRaftState state = load(dir);
        assertThat(state.snapshot().index()).isEqualTo(2);
        assertThat(indices(state)).containsExactly(3L, 4L, 5L);
    }

    @Test
    public void tornRecordAtTheEndIsDropped() throws IOException {
        Path dir = folder.newFolder().toPath();
        FileRaftStateStore store = newStore(dir, null);
        for (int i = 1; i <= 3; i++) {
            store.persistEntry(entry(1, i));
        }
        store.close();
        Path log = dir.resolve(FileRaftStateStore.logFileName(0));
        long length = Files.size(log);
        try (FileChannel channel = FileChannel.open(log, StandardOpenOption.WRITE)) {
            // a crash in the middle of writing the third entry
            channel.truncate(length - 3);
        }

        FileRaftStateLoader loader = new FileRaftStateLoader(dir, serializationService);
        RestoredRaftState state = loader.load();
        assertThat(indices(state)).containsExactly(1L, 2L);

        // the torn bytes are cut off before appending
        store = newStore(dir, loader.logFile());
        store.persistEntry(entry(2, 3));
        store.close();
        assertThat(indices(load(dir))).containsExactly(1L, 2L, 3L);
    }

    @Test
    public void incompleteGenerationFallsBackToThePreviousOne() throws IOException {
        Path dir = folder.newFolder().toPath();
        FileRaftStateStore store = newStore(dir, null);
        for (int i = 1; i <= 3; i++) {
            store.persistEntry(entry(1, i));
        }
        store.close();
        // the member stopped while it was writing the next generation's snapshot
        Files.write(dir.resolve(FileRaftStateStore.logFileName(1)), new byte[]{0, 0, 1});

        assertThat(indices(load(dir))).containsExactly(1L, 2L, 3L);
    }

    private FileRaftStateStore newStore(Path dir, RaftLogFile restored) throws IOException {
        FileRaftStateStore store = new FileRaftStateStore(dir, serializationService, restored);
        store.open();
        store.persistInitialMembers(local, members);
        return store;
    }

    private RestoredRaftState load(Path dir) throws IOException {
        return new FileRaftStateLoader(dir, serializationService).load();
    }

    private static LogEntry entry(int term, long index) {
        return new LogEntry(term, index, "op-" + index);
    }

    private static List<Long> indices(RestoredRaftState state) {
        return Arrays.stream(state.entries()).map(LogEntry::index).toList();
    }
}
