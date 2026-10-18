/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.client.cache.impl.nearcache;

import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.client.cache.impl.HecatonClientCacheManager;
import uk.org.hecaton.client.cache.impl.HecatonClientCachingProvider;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.internal.nearcache.NearCache;
import uk.org.hecaton.internal.nearcache.impl.DefaultNearCache;
import uk.org.hecaton.internal.util.RuntimeAvailableProcessors;
import uk.org.hecaton.nearcache.NearCacheStats;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.spi.CachingProvider;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static uk.org.hecaton.config.NearCacheConfig.LocalUpdatePolicy.INVALIDATE;
import static uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingTask.RECONCILIATION_INTERVAL_SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheNearCacheCacheOnUpdateTest extends ClientNearCacheTestSupport {

    private static final int NUM_OF_KEYS = 100;

    @Test
    public void with_cacheOnUpdate_policy_concurrently_updated_near_cache_does_not_cause_any_miss() {
        ICache<Integer, Integer> nearCachedClientCache = newNearCachedCache(CACHE_ON_UPDATE);
        checkNearCacheInstance(nearCachedClientCache);

        runTest(nearCachedClientCache, CACHE_ON_UPDATE);

        NearCache nearCache = ((NearCachedClientCacheProxy) nearCachedClientCache).getNearCache();
        int size = nearCache.size();
        NearCacheStats nearCacheStatistics = nearCachedClientCache.getLocalCacheStatistics()
                .getNearCacheStatistics();
        assertEquals(size + ", " + nearCacheStatistics.toString()
                + ", " + nearCache.getNearCacheConfig(), 0, nearCacheStatistics.getMisses());
    }

    protected void checkNearCacheInstance(ICache iCacheOnClient) {
        NearCache nearCache = ((NearCachedClientCacheProxy<Object, Object>) iCacheOnClient).getNearCache();
        assertInstanceOf(DefaultNearCache.class, nearCache);
    }

    @Test
    public void with_invalidate_policy_concurrently_updated_near_cache_causes_misses() {
        ICache<Integer, Integer> nearCachedClientCache = newNearCachedCache(INVALIDATE);
        checkNearCacheInstance(nearCachedClientCache);

        runTest(nearCachedClientCache, INVALIDATE);

        NearCacheStats nearCacheStatistics = nearCachedClientCache.getLocalCacheStatistics()
                .getNearCacheStatistics();
        assertTrue(nearCacheStatistics.toString(), nearCacheStatistics.getMisses() > 0);
    }

    private static void runTest(ICache<Integer, Integer> icacheOnClient,
                                NearCacheConfig.LocalUpdatePolicy localUpdatePolicy) {
        for (int i = 0; i < NUM_OF_KEYS; i++) {
            icacheOnClient.put(i, i);

            if (localUpdatePolicy == INVALIDATE) {
                // populate near-cache here for invalidate policy.
                icacheOnClient.get(i);
            }
        }

        assertEquals(NUM_OF_KEYS, ((NearCachedClientCacheProxy) icacheOnClient).getNearCache().size());

        AtomicBoolean stop = new AtomicBoolean();

        Runnable getter = () -> {
            int i = 0;
            while (!stop.get()) {
                icacheOnClient.get(++i % NUM_OF_KEYS);
            }
        };

        Runnable putter = () -> {
            int i = 0;
            while (!stop.get()) {
                i = ++i % NUM_OF_KEYS;
                icacheOnClient.put(i, i);
            }
        };

        ExecutorService executor = Executors.newCachedThreadPool();

        int numOfGetters = 2 * RuntimeAvailableProcessors.get();
        for (int i = 0; i < numOfGetters; i++) {
            executor.execute(getter);
        }

        executor.execute(putter);

        sleepSeconds(10);
        stop.set(true);

        executor.shutdown();
        try {
            executor.awaitTermination(100, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail("InterruptedException");
        } finally {
            executor.shutdownNow();
        }
    }

    private ICache<Integer, Integer> newNearCachedCache(NearCacheConfig.LocalUpdatePolicy localUpdatePolicy) {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(localUpdatePolicy);
        ClientConfig clientConfig = getClientConfig()
                .addNearCacheConfig(nearCacheConfig)
                // Disable the repairing task invalidations to prevent any interference with the test
                .setProperty(RECONCILIATION_INTERVAL_SECONDS.getName(), "0");

        HecatonClientProxy client = (HecatonClientProxy) hecatonFactory.newHecatonClient(clientConfig);
        CachingProvider provider = new HecatonClientCachingProvider(client);
        HecatonClientCacheManager cacheManager = (HecatonClientCacheManager) provider.getCacheManager();

        return cacheManager.createCache(DEFAULT_CACHE_NAME,
                newCacheConfig(InMemoryFormat.BINARY));
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    protected NearCacheConfig getNearCacheConfig(NearCacheConfig.LocalUpdatePolicy localUpdatePolicy) {
        return new NearCacheConfig()
                .setName(DEFAULT_CACHE_NAME)
                .setInMemoryFormat(nearCacheInMemoryFormat())
                .setLocalUpdatePolicy(localUpdatePolicy);
    }

    protected InMemoryFormat nearCacheInMemoryFormat() {
        return InMemoryFormat.BINARY;
    }

    private static <K, V> CacheConfig<K, V> newCacheConfig(InMemoryFormat inMemoryFormat) {
        return new CacheConfig<K, V>()
                .setName(DEFAULT_CACHE_NAME)
                .setInMemoryFormat(inMemoryFormat)
                .setBackupCount(1);
    }

}
