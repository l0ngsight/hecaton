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
package uk.org.hecaton.hd.cache;

import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.EvictionPolicy;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.config.NativeMemoryConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.hd.HdModule;
import uk.org.hecaton.instance.module.NativeMemoryCapability;
import uk.org.hecaton.memory.Capacity;
import uk.org.hecaton.memory.MemoryUnit;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.spi.CachingProvider;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static org.assertj.core.api.Assertions.assertThat;
import static uk.org.hecaton.test.Accessors.getNode;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HdCacheTest extends HecatonTestSupport {

    private static Config nativeConfig() {
        Config config = smallInstanceConfig();
        config.getNativeMemoryConfig().setEnabled(true)
              .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.POOLED)
              .setCapacity(new Capacity(128, MemoryUnit.MEGABYTES));
        return config;
    }

    private static <K, V> ICache<K, V> nativeCache(HecatonInstance member, String name, MaxSizePolicy policy, int size) {
        CachingProvider provider = createServerCachingProvider(member);
        CacheManager manager = provider.getCacheManager();
        CacheConfig<K, V> config = new CacheConfig<K, V>()
                .setInMemoryFormat(InMemoryFormat.NATIVE)
                .setEvictionConfig(new EvictionConfig().setEvictionPolicy(EvictionPolicy.LRU)
                        .setMaxSizePolicy(policy).setSize(size));
        return manager.createCache(name, config).unwrap(ICache.class);
    }

    private static <K, V> ICache<K, V> nativeCache(HecatonInstance member, String name) {
        return nativeCache(member, name, MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE, 99);
    }

    @Test
    public void basicOperations() {
        HecatonInstance member = createHecatonInstance(nativeConfig());
        ICache<Integer, String> cache = nativeCache(member, "basic");
        cache.put(1, "a");
        assertThat(cache.get(1)).isEqualTo("a");
        assertThat(cache.getAndPut(1, "b")).isEqualTo("a");
        assertThat(cache.putIfAbsent(1, "c")).isFalse();
        assertThat(cache.replace(1, "b", "d")).isTrue();
        assertThat(cache.containsKey(1)).isTrue();
        assertThat(cache.getAndRemove(1)).isEqualTo("d");
        assertThat(cache.get(1)).isNull();
        for (int i = 0; i < 1000; i++) {
            cache.put(i, "v" + i);
        }
        assertThat(cache.size()).isEqualTo(1000);
        Set<Integer> keys = new HashSet<>();
        for (Cache.Entry<Integer, String> entry : cache) {
            keys.add(entry.getKey());
            assertThat(entry.getValue()).isEqualTo("v" + entry.getKey());
        }
        assertThat(keys).hasSize(1000);
        assertThat(cache.<String>invoke(5, (entry, args) -> {
            entry.setValue(entry.getValue() + "!");
            return entry.getValue();
        })).isEqualTo("v5!");
        cache.removeAll();
        assertThat(cache.size()).isZero();
        assertThat(usedNative(member)).isZero();
    }

    @Test
    public void expiryPolicyApplies() {
        HecatonInstance member = createHecatonInstance(nativeConfig());
        ICache<Integer, String> cache = nativeCache(member, "expiring");
        cache.put(1, "short", new CreatedExpiryPolicy(new Duration(TimeUnit.SECONDS, 1)));
        cache.put(2, "long");
        assertTrueEventually(() -> assertThat(cache.get(1)).isNull());
        assertThat(cache.get(2)).isEqualTo("long");
    }

    @Test
    public void usedNativeMemorySizeLimitEvicts() {
        HecatonInstance member = createHecatonInstance(nativeConfig());
        ICache<Integer, byte[]> cache = nativeCache(member, "bounded", MaxSizePolicy.USED_NATIVE_MEMORY_SIZE, 2);
        for (int i = 0; i < 10_000; i++) {
            cache.put(i, new byte[1000]);
        }
        HdModule module = module(member);
        long footprint = module.cacheFootprint("/hz/bounded").sum();
        // one entry over the limit per partition at most
        assertThat(footprint).isLessThan(3L << 20);
        assertThat(cache.size()).isLessThan(10_000);
    }

    @Test
    public void migrationKeepsTheData() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        Config config = nativeConfig();
        HecatonInstance first = factory.newHecatonInstance(config);
        ICache<Integer, String> cache = nativeCache(first, "migrating");
        for (int i = 0; i < 2000; i++) {
            cache.put(i, "v" + i);
        }
        HecatonInstance second = factory.newHecatonInstance(config);
        HecatonInstance third = factory.newHecatonInstance(config);
        waitAllForSafeState(first, second, third);
        first.getLifecycleService().terminate();
        waitAllForSafeState(second, third);
        CachingProvider provider = createServerCachingProvider(second);
        Cache<Integer, String> survivor = provider.getCacheManager().getCache("migrating");
        int count = 0;
        for (Cache.Entry<Integer, String> entry : survivor) {
            assertThat(entry.getValue()).isEqualTo("v" + entry.getKey());
            count++;
        }
        assertThat(count).isEqualTo(2000);
    }

    @Test
    public void destroyReturnsTheMemory() {
        HecatonInstance member = createHecatonInstance(nativeConfig());
        ICache<Integer, byte[]> cache = nativeCache(member, "destroyed");
        for (int i = 0; i < 1000; i++) {
            cache.put(i, new byte[500]);
        }
        assertThat(usedNative(member)).isGreaterThan(500_000);
        cache.destroy();
        assertThat(usedNative(member)).isZero();
    }

    private static HdModule module(HecatonInstance member) {
        return (HdModule) getNode(member).getMemberModules().require(NativeMemoryCapability.class, "test");
    }

    private static long usedNative(HecatonInstance member) {
        return module(member).memoryManager().usedNative();
    }
}
