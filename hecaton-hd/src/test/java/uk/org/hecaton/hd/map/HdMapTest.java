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
package uk.org.hecaton.hd.map;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.EvictionPolicy;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.config.NativeMemoryConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.hd.HdModule;
import uk.org.hecaton.instance.module.NativeMemoryCapability;
import uk.org.hecaton.internal.memory.MemoryStats;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.memory.Capacity;
import uk.org.hecaton.memory.MemoryUnit;
import uk.org.hecaton.memory.NativeOutOfMemoryError;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static uk.org.hecaton.test.Accessors.getNode;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HdMapTest extends HecatonTestSupport {

    static Config nativeConfig(NativeMemoryConfig.MemoryAllocatorType allocator) {
        Config config = smallInstanceConfig();
        config.getNativeMemoryConfig().setEnabled(true).setAllocatorType(allocator)
              .setCapacity(new Capacity(128, MemoryUnit.MEGABYTES));
        config.addMapConfig(new MapConfig("native*").setInMemoryFormat(InMemoryFormat.NATIVE));
        config.addMapConfig(new MapConfig("evicting*").setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(new EvictionConfig()
                        .setEvictionPolicy(EvictionPolicy.LRU)
                        .setMaxSizePolicy(MaxSizePolicy.PER_PARTITION)
                        .setSize(10)));
        return config;
    }

    @Test
    public void basicOperations() {
        HecatonInstance member = createHecatonInstance(nativeConfig(NativeMemoryConfig.MemoryAllocatorType.POOLED));
        IMap<Integer, String> map = member.getMap("native");
        assertThat(map.put(1, "a")).isNull();
        assertThat(map.put(1, "b")).isEqualTo("a");
        assertThat(map.putIfAbsent(1, "c")).isEqualTo("b");
        assertThat(map.replace(1, "b", "d")).isTrue();
        assertThat(map.get(1)).isEqualTo("d");
        map.set(2, "e");
        assertThat(usedNative(member)).isGreaterThan(0);
        assertThat(map.containsKey(2)).isTrue();
        assertThat(map.containsValue("e")).isTrue();
        assertThat(map.remove(2)).isEqualTo("e");
        assertThat(map.get(2)).isNull();
        map.delete(1);
        assertThat(map.isEmpty()).isTrue();
        assertThat(usedNative(member)).isZero();
    }

    @Test
    public void manyEntriesIterationAndQueries() {
        HecatonInstance member = createHecatonInstance(nativeConfig(NativeMemoryConfig.MemoryAllocatorType.STANDARD));
        IMap<Integer, Integer> map = member.getMap("native-many");
        Map<Integer, Integer> expected = new HashMap<>();
        for (int i = 0; i < 20_000; i++) {
            expected.put(i, i * 2);
        }
        map.putAll(expected);
        assertThat(map.size()).isEqualTo(20_000);
        assertThat(new HashSet<>(map.keySet())).isEqualTo(expected.keySet());
        assertThat(map.entrySet()).hasSize(20_000);
        assertThat(map.values(Predicates.lessThan("this", 20))).containsExactlyInAnyOrder(0, 2, 4, 6, 8, 10, 12, 14, 16, 18);
        assertThat(map.keySet(Predicates.sql("this >= 39990"))).containsExactlyInAnyOrder(19995, 19996, 19997, 19998, 19999);
        Set<Integer> iterated = new HashSet<>();
        map.forEach((k, v) -> iterated.add(k));
        assertThat(iterated).hasSize(20_000);
    }

    @Test
    public void entryProcessorsAndAtomicOperations() {
        HecatonInstance member = createHecatonInstance(nativeConfig(NativeMemoryConfig.MemoryAllocatorType.POOLED));
        IMap<String, Integer> map = member.getMap("native-ep");
        for (int i = 0; i < 100; i++) {
            map.put("k" + i, i);
        }
        map.executeOnEntries(entry -> {
            entry.setValue(entry.getValue() + 1000);
            return null;
        });
        assertThat(map.get("k7")).isEqualTo(1007);
        map.executeOnKey("k8", entry -> {
            entry.setValue(null);
            return null;
        });
        assertThat(map.containsKey("k8")).isFalse();
        assertThat(map.size()).isEqualTo(99);
    }

    @Test
    public void ttlExpires() {
        HecatonInstance member = createHecatonInstance(nativeConfig(NativeMemoryConfig.MemoryAllocatorType.POOLED));
        IMap<Integer, String> map = member.getMap("native-ttl");
        map.put(1, "short", 1, TimeUnit.SECONDS);
        map.put(2, "long");
        assertTrueEventually(() -> assertThat(map.get(1)).isNull());
        assertThat(map.get(2)).isEqualTo("long");
    }

    @Test
    public void evictionKeepsTheLimit() {
        HecatonInstance member = createHecatonInstance(nativeConfig(NativeMemoryConfig.MemoryAllocatorType.POOLED));
        IMap<Integer, Integer> map = member.getMap("evicting");
        for (int i = 0; i < 20_000; i++) {
            map.put(i, i);
        }
        int partitions = member.getPartitionService().getPartitions().size();
        assertThat(map.size()).isLessThanOrEqualTo(10 * partitions);
    }

    @Test
    public void backupsAndMigrationKeepTheData() {
        Config config = nativeConfig(NativeMemoryConfig.MemoryAllocatorType.POOLED);
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        HecatonInstance first = factory.newHecatonInstance(config);
        IMap<Integer, String> map = first.getMap("native-migrating");
        for (int i = 0; i < 5000; i++) {
            map.put(i, "v" + i);
        }
        HecatonInstance second = factory.newHecatonInstance(config);
        HecatonInstance third = factory.newHecatonInstance(config);
        waitAllForSafeState(first, second, third);
        first.getLifecycleService().terminate();
        waitAllForSafeState(second, third);
        IMap<Integer, String> survivor = second.getMap("native-migrating");
        assertThat(survivor.size()).isEqualTo(5000);
        for (int i = 0; i < 5000; i += 97) {
            assertThat(survivor.get(i)).isEqualTo("v" + i);
        }
    }

    @Test
    public void memoryIsReturnedWhenTheMapIsCleared() {
        HecatonInstance member = createHecatonInstance(nativeConfig(NativeMemoryConfig.MemoryAllocatorType.STANDARD));
        IMap<Integer, byte[]> map = member.getMap("native-clear");
        for (int i = 0; i < 1000; i++) {
            map.put(i, new byte[1000]);
        }
        assertThat(usedNative(member)).isGreaterThan(1_000_000);
        map.clear();
        assertThat(usedNative(member)).isZero();
        map.put(1, new byte[10]);
        map.destroy();
        assertThat(usedNative(member)).isZero();
    }

    @Test
    public void memoryStatsReportNativeMemory() {
        HecatonInstance member = createHecatonInstance(nativeConfig(NativeMemoryConfig.MemoryAllocatorType.POOLED));
        MemoryStats stats = getNode(member).getNodeExtension().getMemoryStats();
        assertThat(stats.getMaxNative()).isEqualTo(128L << 20);
        member.getMap("native-stats").put(1, "x");
        assertThat(stats.getUsedNative()).isGreaterThan(0);
        assertThat(stats.getCommittedNative()).isGreaterThanOrEqualTo(stats.getUsedNative());
    }

    @Test
    public void runningOutOfNativeMemoryEvictsOrFails() {
        Config config = smallInstanceConfig();
        config.getNativeMemoryConfig().setEnabled(true)
              .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED)
              .setCapacity(new Capacity(16, MemoryUnit.MEGABYTES))
              // each partition thread needs pages of its own
              .setPageSize(1 << 16);
        config.addMapConfig(new MapConfig("forced").setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(new EvictionConfig()
                        .setEvictionPolicy(EvictionPolicy.LRU)
                        .setMaxSizePolicy(MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE)
                        .setSize(100)));
        config.addMapConfig(new MapConfig("unbounded").setInMemoryFormat(InMemoryFormat.NATIVE));
        HecatonInstance member = createHecatonInstance(config);

        IMap<Integer, byte[]> forced = member.getMap("forced");
        for (int i = 0; i < 40_000; i++) {
            forced.set(i, new byte[1000]);
        }
        assertThat(forced.size()).isLessThan(40_000);
        forced.destroy();

        IMap<Integer, byte[]> unbounded = member.getMap("unbounded");
        assertThatThrownBy(() -> {
            for (int i = 0; i < 40_000; i++) {
                unbounded.set(i, new byte[1000]);
            }
        }).hasCauseInstanceOf(NativeOutOfMemoryError.class);
    }

    static long usedNative(HecatonInstance member) {
        HdModule module = (HdModule) getNode(member).getMemberModules().require(NativeMemoryCapability.class, "test");
        return module.memoryManager().usedNative();
    }
}
