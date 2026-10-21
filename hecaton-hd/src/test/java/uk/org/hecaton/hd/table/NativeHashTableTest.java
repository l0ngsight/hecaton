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
package uk.org.hecaton.hd.table;

import uk.org.hecaton.config.NativeMemoryConfig;
import uk.org.hecaton.hd.memory.HdMemoryManager;
import uk.org.hecaton.hd.memory.NativeAccess;
import uk.org.hecaton.hd.memory.NativeData;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.memory.Capacity;
import uk.org.hecaton.memory.MemoryUnit;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NativeHashTableTest {

    private final InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
    private final HdMemoryManager memory = HdMemoryManager.create(new NativeMemoryConfig().setEnabled(true)
            .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD)
            .setCapacity(new Capacity(256, MemoryUnit.MEGABYTES)));
    private final NativeHashTable table = new NativeHashTable(memory);
    /** key -> entry block, the model */
    private final Map<Data, Long> model = new HashMap<>();

    @After
    public void tearDown() {
        table.clear(this::freeEntry);
        table.dispose();
        assertThat(memory.usedNative()).isZero();
        assertThat(memory.usedMetadata()).isZero();
    }

    @Test
    public void behavesLikeAMap() {
        Random random = new Random(3);
        for (int i = 0; i < 50_000; i++) {
            Data key = ss.toData(random.nextInt(5000));
            int hash = NativeHashTable.hashOf(key);
            Long expected = model.get(key);
            assertThat(table.get(hash, key)).isEqualTo(expected == null ? 0 : expected);
            switch (random.nextInt(3)) {
                case 0, 1 -> {
                    if (expected == null) {
                        long entry = newEntry(key);
                        table.insert(hash, entry);
                        model.put(key, entry);
                    }
                }
                default -> {
                    long removed = table.remove(hash, key);
                    assertThat(removed).isEqualTo(expected == null ? 0 : expected);
                    if (expected != null) {
                        model.remove(key);
                        freeEntry(removed);
                    }
                }
            }
            assertThat(table.size()).isEqualTo(model.size());
        }
        for (Map.Entry<Data, Long> e : model.entrySet()) {
            assertThat(table.get(NativeHashTable.hashOf(e.getKey()), e.getKey())).isEqualTo(e.getValue());
        }
    }

    @Test
    public void iterationSeesEveryEntryOnceAcrossResizes() {
        for (int i = 0; i < 1000; i++) {
            add(i);
        }
        NativeHashTable.Cursor cursor = new NativeHashTable.Cursor();
        Set<Data> seen = new HashSet<>();
        int step = 0;
        long entry;
        while ((entry = table.next(cursor)) != 0) {
            assertThat(seen.add(NativeData.read(NativeHashTable.keyOf(entry)))).isTrue();
            // grow and shrink the table while iterating, with keys outside the first 1000
            if (++step % 100 == 0) {
                for (int i = 0; i < 2000; i++) {
                    add(100_000 + step * 10_000 + i);
                }
                for (int i = 0; i < 2000; i++) {
                    remove(100_000 + step * 10_000 + i);
                }
            }
        }
        for (int i = 0; i < 1000; i++) {
            assertThat(seen).contains(ss.toData(i));
        }
    }

    @Test
    public void iterationToleratesRemovalOfReturnedEntries() {
        for (int i = 0; i < 500; i++) {
            add(i);
        }
        NativeHashTable.Cursor cursor = new NativeHashTable.Cursor();
        int count = 0;
        long entry;
        while ((entry = table.next(cursor)) != 0) {
            Data key = NativeData.read(NativeHashTable.keyOf(entry));
            freeEntry(table.remove(NativeHashTable.hashOf(key), key));
            model.remove(key);
            count++;
        }
        assertThat(count).isEqualTo(500);
        assertThat(table.size()).isZero();
    }

    @Test
    public void samplesAreEntries() {
        for (int i = 0; i < 100; i++) {
            add(i);
        }
        long[] samples = new long[15];
        int found = table.sample(12345, samples, 15);
        assertThat(found).isEqualTo(15);
        Set<Long> entries = new HashSet<>(model.values());
        for (int i = 0; i < found; i++) {
            assertThat(entries).contains(samples[i]);
        }
        assertThat(table.sample(1, new long[200], 200)).isEqualTo(100);
    }

    @Test
    public void footprintShrinksWhenEmptied() {
        long initial = table.footprint();
        List<Integer> keys = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            add(i);
            keys.add(i);
        }
        assertThat(table.footprint()).isGreaterThan(initial);
        for (int key : keys) {
            remove(key);
        }
        assertThat(table.footprint()).isEqualTo(initial);
    }

    private void add(int value) {
        Data key = ss.toData(value);
        long entry = newEntry(key);
        table.insert(NativeHashTable.hashOf(key), entry);
        model.put(key, entry);
    }

    private void remove(int value) {
        Data key = ss.toData(value);
        freeEntry(table.remove(NativeHashTable.hashOf(key), key));
        model.remove(key);
    }

    /** An entry block holding only its key block's address. */
    private long newEntry(Data key) {
        long entry = memory.allocate(Long.BYTES);
        NativeAccess.putLong(entry, NativeData.write(memory, key));
        return entry;
    }

    private void freeEntry(long entry) {
        NativeData.free(memory, NativeHashTable.keyOf(entry));
        memory.free(entry, Long.BYTES);
    }
}
