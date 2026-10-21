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

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.EvictionPolicy;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.config.NativeMemoryConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.memory.MemoryStats;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.memory.Capacity;
import uk.org.hecaton.memory.MemoryUnit;
import uk.org.hecaton.nearcache.NearCacheStats;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.client.impl.clientside.ClientTestUtil.getHecatonClientInstanceImpl;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Client Near Caches with the {@code NATIVE} format (F11): with {@code hecaton-hd} on a Java 25
 * client, their values are in the client's native memory.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HdClientNearCacheTest extends HecatonTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    private static ClientConfig clientConfig(MaxSizePolicy policy, int size) {
        ClientConfig config = new ClientConfig();
        config.getNativeMemoryConfig().setEnabled(true)
              .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD)
              .setCapacity(new Capacity(32, MemoryUnit.MEGABYTES));
        config.addNearCacheConfig(new NearCacheConfig("near*").setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(new EvictionConfig().setEvictionPolicy(EvictionPolicy.LRU)
                        .setMaxSizePolicy(policy).setSize(size)));
        return config;
    }

    @Test
    public void readsAreServedFromNativeMemoryAndInvalidated() {
        HecatonInstance member = factory.newHecatonInstance(smallInstanceConfig());
        HecatonInstance client = factory.newHecatonClient(clientConfig(MaxSizePolicy.ENTRY_COUNT, 10_000));
        IMap<Integer, String> written = member.getMap("near");
        IMap<Integer, String> read = client.getMap("near");
        for (int i = 0; i < 100; i++) {
            written.put(i, "v" + i);
        }
        for (int round = 0; round < 2; round++) {
            for (int i = 0; i < 100; i++) {
                assertThat(read.get(i)).isEqualTo("v" + i);
            }
        }
        NearCacheStats stats = read.getLocalMapStats().getNearCacheStats();
        assertThat(stats.getHits()).isGreaterThan(50);
        MemoryStats memory = getHecatonClientInstanceImpl(client).getClientExtension().getMemoryStats();
        assertThat(memory.getUsedNative()).isGreaterThan(0);

        written.put(1, "changed");
        assertTrueEventually(() -> assertThat(read.get(1)).isEqualTo("changed"));
    }

    @Test
    public void nativeMemorySizeLimitEvicts() {
        HecatonInstance member = factory.newHecatonInstance(smallInstanceConfig());
        HecatonInstance client = factory.newHecatonClient(clientConfig(MaxSizePolicy.USED_NATIVE_MEMORY_SIZE, 1));
        IMap<Integer, byte[]> written = member.getMap("near-bounded");
        for (int i = 0; i < 3000; i++) {
            written.put(i, new byte[1000]);
        }
        IMap<Integer, byte[]> read = client.getMap("near-bounded");
        for (int i = 0; i < 3000; i++) {
            read.get(i);
        }
        NearCacheStats stats = read.getLocalMapStats().getNearCacheStats();
        assertThat(stats.getOwnedEntryCount()).isLessThan(3000);
        assertThat(stats.getEvictions()).isGreaterThan(0);
    }
}
