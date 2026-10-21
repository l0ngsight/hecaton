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

import uk.org.hecaton.config.NativeMemoryConfig;
import uk.org.hecaton.hd.memory.HdMemoryManager;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.memory.Capacity;
import uk.org.hecaton.memory.MemoryUnit;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionTiersTest {

    private static final int COUNT = 1000;
    private static final int VALUE_SIZE = 1000;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final SerializationService serializationService = new DefaultSerializationServiceBuilder().build();
    private final HdMemoryManager memory = HdMemoryManager.create(new NativeMemoryConfig().setEnabled(true)
            .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD)
            .setCapacity(new Capacity(64, MemoryUnit.MEGABYTES)));

    @Test
    public void leastRecentlyUsedValuesMoveToDiskAndBack() throws Exception {
        MemoryTierBudget budget = new MemoryTierBudget(100 * 1024);
        Path directory = folder.newFolder().toPath();
        PartitionTiers tiers = new PartitionTiers(memory, budget, directory, 3);
        List<TieredRecord> records = fill(tiers);

        assertThat(budget.used()).isLessThanOrEqualTo(budget.capacity());
        assertThat(tiers.memoryCount()).isGreaterThan(50);
        assertThat(tiers.diskCount()).isEqualTo(COUNT - tiers.memoryCount());
        assertThat(files(directory)).isNotEmpty();
        // the latest values are in memory, the first ones on disk
        assertThat(records.get(COUNT - 1).inMemory()).isTrue();
        assertThat(records.get(0).onDisk()).isTrue();

        // reading a value on the partition's thread brings it back to memory
        assertThat(value(records.get(0))).isEqualTo(value(0, 0));
        assertThat(records.get(0).inMemory()).isTrue();
        for (int i = 0; i < COUNT; i++) {
            assertThat(value(records.get(i))).isEqualTo(value(i, 0));
        }
        assertThat(budget.used()).isLessThanOrEqualTo(budget.capacity());

        tiers.clear();
        assertThat(budget.used()).isZero();
        assertThat(files(directory)).isEmpty();
    }

    @Test
    public void diskTierIsCompacted() throws Exception {
        MemoryTierBudget budget = new MemoryTierBudget(0);
        Path directory = folder.newFolder().toPath();
        PartitionTiers tiers = new PartitionTiers(memory, budget, directory, 0);
        List<TieredRecord> records = fill(tiers);
        for (int round = 1; round <= 3; round++) {
            for (int i = 0; i < COUNT; i++) {
                records.get(i).setValue(serializationService.toData(value(i, round)));
            }
        }
        // three rounds of rewrites left garbage, which compaction removed
        assertThat(tiers.disk().garbage()).isLessThanOrEqualTo(tiers.disk().live());
        assertThat(tiers.disk().file().getFileName().toString()).isNotEqualTo("0-0.tier");
        assertThat(files(directory)).hasSize(1);
        for (int i = 0; i < COUNT; i += 7) {
            assertThat(value(records.get(i))).isEqualTo(value(i, 3));
        }
        // removing every record leaves nothing
        for (TieredRecord record : records) {
            tiers.detach(record);
        }
        assertThat(tiers.memoryCount()).isZero();
        assertThat(tiers.diskCount()).isZero();
        assertThat(budget.used()).isZero();
        tiers.clear();
    }

    private List<TieredRecord> fill(PartitionTiers tiers) {
        List<TieredRecord> records = new ArrayList<>();
        for (int i = 0; i < COUNT; i++) {
            TieredRecord record = new TieredRecord(serializationService.toData(value(i, 0)));
            tiers.attach(record);
            records.add(record);
        }
        return records;
    }

    private String value(TieredRecord record) {
        Data data = record.getValue();
        return serializationService.toObject(data);
    }

    private static String value(int i, int round) {
        return (i + "-" + round + "-").repeat(VALUE_SIZE).substring(0, VALUE_SIZE);
    }

    private static List<Path> files(Path directory) throws Exception {
        if (!Files.exists(directory)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(directory)) {
            return files.toList();
        }
    }
}
