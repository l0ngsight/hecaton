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
package uk.org.hecaton.hd.nearcache;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.EvictionPolicy;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.config.NativeMemoryConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.memory.Capacity;
import uk.org.hecaton.memory.MemoryUnit;
import uk.org.hecaton.nearcache.NearCacheStats;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HdNearCacheTest extends HecatonTestSupport {

    private static Config config(MaxSizePolicy policy, int size) {
        Config config = smallInstanceConfig();
        config.getNativeMemoryConfig().setEnabled(true)
              .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED)
              .setCapacity(new Capacity(64, MemoryUnit.MEGABYTES));
        NearCacheConfig nearCacheConfig = new NearCacheConfig().setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(new EvictionConfig().setEvictionPolicy(EvictionPolicy.LRU)
                        .setMaxSizePolicy(policy).setSize(size));
        config.addMapConfig(new MapConfig("near*").setNearCacheConfig(nearCacheConfig));
        return config;
    }

    @Test
    public void readsAreServedFromTheNearCacheAndInvalidated() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        Config config = config(MaxSizePolicy.ENTRY_COUNT, 10_000);
        HecatonInstance writer = factory.newHecatonInstance(config);
        HecatonInstance reader = factory.newHecatonInstance(config);
        IMap<Integer, String> written = writer.getMap("near");
        IMap<Integer, String> read = reader.getMap("near");
        for (int i = 0; i < 100; i++) {
            written.put(i, "v" + i);
        }
        for (int i = 0; i < 100; i++) {
            assertThat(read.get(i)).isEqualTo("v" + i);
        }
        for (int i = 0; i < 100; i++) {
            assertThat(read.get(i)).isEqualTo("v" + i);
        }
        NearCacheStats stats = read.getLocalMapStats().getNearCacheStats();
        assertThat(stats.getHits()).isGreaterThan(0);

        written.put(1, "changed");
        assertTrueEventually(() -> assertThat(read.get(1)).isEqualTo("changed"));
    }

    @Test
    public void nativeMemorySizeLimitEvicts() {
        Config config = config(MaxSizePolicy.USED_NATIVE_MEMORY_SIZE, 1);
        // a single member caches its own entries only when asked to
        config.getMapConfig("near-bounded").getNearCacheConfig().setCacheLocalEntries(true);
        HecatonInstance member = createHecatonInstance(config);
        IMap<Integer, byte[]> map = member.getMap("near-bounded");
        for (int i = 0; i < 3000; i++) {
            map.put(i, new byte[1000]);
        }
        for (int i = 0; i < 3000; i++) {
            map.get(i);
        }
        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertThat(stats.getOwnedEntryCount()).isLessThan(3000);
        assertThat(stats.getEvictions()).isGreaterThan(0);
    }
}
