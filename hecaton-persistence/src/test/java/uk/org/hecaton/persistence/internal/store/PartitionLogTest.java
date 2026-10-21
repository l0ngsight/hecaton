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

import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionLogTest {

    private static final LogSettings SMALL_CHUNKS = new LogSettings(1 << 12, 0.5);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private PartitionLog log;

    @After
    public void tearDown() {
        if (log != null) {
            log.close();
        }
    }

    @Test
    public void entriesSurviveReopening() throws IOException {
        Path dir = folder.newFolder().toPath().resolve("p0");
        log = PartitionLog.open(dir, LogSettings.DEFAULT);
        log.put(1, bytes("a"), bytes("1"));
        log.put(1, bytes("b"), bytes("2"));
        log.put(1, bytes("a"), bytes("3"));
        log.put(2, bytes("a"), bytes("4"));
        log.remove(1, bytes("b"));
        log.close();

        log = PartitionLog.open(dir, LogSettings.DEFAULT);
        assertThat(contents(log)).isEqualTo(Map.of("1/a", "3", "2/a", "4"));
        assertThat(log.size(1)).isEqualTo(1);
        assertThat(log.structures()).containsExactlyInAnyOrder(1, 2);
    }

    @Test
    public void clearRemovesTheEntriesOfOneStructure() throws IOException {
        Path dir = folder.newFolder().toPath();
        log = PartitionLog.open(dir, LogSettings.DEFAULT);
        log.put(1, bytes("a"), bytes("1"));
        log.put(2, bytes("a"), bytes("2"));
        log.clear(1);
        log.put(1, bytes("b"), bytes("3"));
        log.close();

        log = PartitionLog.open(dir, LogSettings.DEFAULT);
        assertThat(contents(log)).isEqualTo(Map.of("1/b", "3", "2/a", "2"));
    }

    @Test
    public void tornRecordAtTheEndIsTruncated() throws IOException {
        Path dir = folder.newFolder().toPath();
        log = PartitionLog.open(dir, LogSettings.DEFAULT);
        log.put(1, bytes("a"), bytes("1"));
        log.put(1, bytes("b"), bytes("2"));
        log.close();
        Path chunk = onlyChunk(dir);
        long size = Files.size(chunk);
        try (FileChannel channel = FileChannel.open(chunk, StandardOpenOption.WRITE)) {
            // half of the last record was written
            channel.truncate(size - 5);
        }

        log = PartitionLog.open(dir, LogSettings.DEFAULT);
        assertThat(contents(log)).isEqualTo(Map.of("1/a", "1"));
        log.put(1, bytes("c"), bytes("3"));
        log.close();
        log = PartitionLog.open(dir, LogSettings.DEFAULT);
        assertThat(contents(log)).isEqualTo(Map.of("1/a", "1", "1/c", "3"));
    }

    @Test
    public void recordWithBadChecksumAtTheEndIsTruncated() throws IOException {
        Path dir = folder.newFolder().toPath();
        log = PartitionLog.open(dir, LogSettings.DEFAULT);
        log.put(1, bytes("a"), bytes("1"));
        log.put(1, bytes("b"), bytes("2"));
        log.close();
        Path chunk = onlyChunk(dir);
        flipLastByte(chunk);

        log = PartitionLog.open(dir, LogSettings.DEFAULT);
        assertThat(contents(log)).isEqualTo(Map.of("1/a", "1"));
    }

    @Test
    public void corruptedClosedChunkIsRefused() throws IOException {
        Path dir = folder.newFolder().toPath();
        log = PartitionLog.open(dir, SMALL_CHUNKS);
        for (int i = 0; log.chunkCount() < 3; i++) {
            log.put(1, bytes("key" + i), new byte[100]);
        }
        log.close();
        log = null;
        flipLastByte(chunks(dir).get(0));

        assertThatThrownBy(() -> PartitionLog.open(dir, SMALL_CHUNKS))
                .isInstanceOf(PersistenceStoreException.class)
                .hasMessageContaining("corrupted");
    }

    @Test
    public void compactionDropsGarbageAndKeepsTheLiveEntries() throws IOException {
        Path dir = folder.newFolder().toPath();
        log = PartitionLog.open(dir, SMALL_CHUNKS);
        Map<String, String> expected = new HashMap<>();
        for (int i = 0; i < 2000; i++) {
            String key = "key" + (i % 10);
            log.put(1, bytes(key), bytes("value" + i));
            expected.put("1/" + key, "value" + i);
        }
        // without compaction, 2000 records of about 40 bytes need about 20 chunks of 4 KB
        assertThat(log.chunkCount()).isLessThan(6);
        assertThat(log.totalBytes()).isLessThan(5 * SMALL_CHUNKS.maxChunkSize());
        assertThat(contents(log)).isEqualTo(expected);
        log.close();

        log = PartitionLog.open(dir, SMALL_CHUNKS);
        assertThat(contents(log)).isEqualTo(expected);
    }

    @Test
    public void chunksSupersededByACompactedChunkAreDeletedOnOpening() throws IOException {
        Path dir = folder.newFolder().toPath();
        Path saved = folder.newFolder().toPath();
        log = PartitionLog.open(dir, SMALL_CHUNKS);
        int count = 0;
        while (log.chunkCount() < 3) {
            log.put(1, bytes("key" + count++), new byte[100]);
        }
        for (int i = 0; i < count; i += 4) {
            log.remove(1, bytes("key" + i));
        }
        List<Path> before = chunks(dir);
        for (Path chunk : before) {
            Files.copy(chunk, saved.resolve(chunk.getFileName()));
        }
        Map<String, String> expected = contents(log);
        log.compact();
        log.close();
        // as if the member failed after the compacted chunk replaced the newest closed chunk,
        // before the older ones were deleted
        assertThat(Files.exists(before.get(0))).isFalse();
        Files.copy(saved.resolve(before.get(0).getFileName()), before.get(0));

        log = PartitionLog.open(dir, SMALL_CHUNKS);
        assertThat(Files.exists(before.get(0))).isFalse();
        assertThat(contents(log)).isEqualTo(expected);
    }

    @Test
    public void randomOperationsMatchAModel() throws IOException {
        Path dir = folder.newFolder().toPath();
        Random random = new Random(42);
        Map<String, String> model = new HashMap<>();
        log = PartitionLog.open(dir, SMALL_CHUNKS);
        for (int i = 0; i < 20_000; i++) {
            int structure = random.nextInt(3);
            String key = "k" + random.nextInt(200);
            int op = random.nextInt(100);
            if (op < 60) {
                String value = "v" + i + "x".repeat(random.nextInt(50));
                log.put(structure, bytes(key), bytes(value));
                model.put(structure + "/" + key, value);
            } else if (op < 95) {
                log.remove(structure, bytes(key));
                model.remove(structure + "/" + key);
            } else if (op < 96) {
                log.clear(structure);
                model.keySet().removeIf(k -> k.startsWith(structure + "/"));
            } else if (op < 98) {
                log.close();
                log = PartitionLog.open(dir, SMALL_CHUNKS);
            }
        }
        assertThat(contents(log)).isEqualTo(model);
        log.close();
        log = PartitionLog.open(dir, SMALL_CHUNKS);
        assertThat(contents(log)).isEqualTo(model);
        long live = model.entrySet().stream()
                .mapToLong(e -> ChunkFormat.recordSize(e.getKey().length() - 2, e.getValue().length())).sum();
        assertThat(log.liveBytes()).isEqualTo(live);
    }

    @Test
    public void deleteRemovesTheFiles() throws IOException {
        Path dir = folder.newFolder().toPath().resolve("p1");
        log = PartitionLog.open(dir, LogSettings.DEFAULT);
        log.put(1, bytes("a"), bytes("1"));
        log.delete();
        log = null;
        assertThat(Files.exists(dir)).isFalse();
    }

    private static Map<String, String> contents(PartitionLog log) {
        Map<String, String> contents = new HashMap<>();
        log.replay((structure, key, value) -> {
            String previous = contents.put(structure + "/" + string(key), string(value));
            assertThat(previous).as("one live record per key").isNull();
        });
        return contents;
    }

    private static List<Path> chunks(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return new ArrayList<>(files.filter(f -> f.toString().endsWith(".chunk")).sorted().toList());
        }
    }

    private static Path onlyChunk(Path dir) throws IOException {
        List<Path> chunks = chunks(dir);
        assertThat(chunks).hasSize(1);
        return chunks.get(0);
    }

    private static void flipLastByte(Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer last = ByteBuffer.allocate(1);
            channel.read(last, channel.size() - 1);
            last.put(0, (byte) (last.get(0) ^ 1));
            channel.write(last.rewind(), channel.size() - 1);
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String string(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
