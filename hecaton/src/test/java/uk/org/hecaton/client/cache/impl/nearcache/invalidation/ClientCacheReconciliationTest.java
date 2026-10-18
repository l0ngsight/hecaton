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

package uk.org.hecaton.client.cache.impl.nearcache.invalidation;

import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.client.cache.impl.nearcache.NearCachedClientCacheProxy;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.nearcache.NearCacheStats;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import static uk.org.hecaton.cache.CacheTestSupport.createClientCachingProvider;
import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static uk.org.hecaton.config.MaxSizePolicy.ENTRY_COUNT;
import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingTask.MAX_TOLERATED_MISS_COUNT;
import static uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingTask.MIN_RECONCILIATION_INTERVAL_SECONDS;
import static uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingTask.RECONCILIATION_INTERVAL_SECONDS;
import static uk.org.hecaton.map.impl.nearcache.invalidation.MemberMapReconciliationTest.assertStats;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;
import static java.lang.Integer.MAX_VALUE;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheReconciliationTest extends HecatonTestSupport {

    private static final String CACHE_1_NAME = "1_ClientCacheReconciliationTest-1";
    private static final String CACHE_2_NAME = "2_ClientCacheReconciliationTest-2";
    private static final String CACHE_3_NAME = "3_ClientCacheReconciliationTest-3";

    private static final int RECONCILIATION_INTERVAL_SECS = 3;

    private final TestHecatonFactory factory = new TestHecatonFactory();

    private CacheConfig<Integer, Integer> cacheConfig;
    private ClientConfig clientConfig;

    // servers
    private Cache<Integer, Integer> serverCache1;
    private Cache<Integer, Integer> serverCache2;
    private Cache<Integer, Integer> serverCache3;
    // clients
    private Cache<Integer, Integer> clientCache1;
    private Cache<Integer, Integer> clientCache2;
    private Cache<Integer, Integer> clientCache3;

    @Before
    public void setUp() {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);

        cacheConfig = new CacheConfig<Integer, Integer>()
                .setEvictionConfig(evictionConfig);

        NearCacheConfig nearCacheConfig = new NearCacheConfig("*")
                .setInvalidateOnChange(true);

        clientConfig = new ClientConfig()
                .setProperty(MAX_TOLERATED_MISS_COUNT.getName(), "0")
                .setProperty(RECONCILIATION_INTERVAL_SECONDS.getName(), String.valueOf(RECONCILIATION_INTERVAL_SECS))
                .setProperty(MIN_RECONCILIATION_INTERVAL_SECONDS.getName(), String.valueOf(RECONCILIATION_INTERVAL_SECS))
                .addNearCacheConfig(nearCacheConfig);

        Config config = getBaseConfig()
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), String.valueOf(Integer.MAX_VALUE))
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), String.valueOf(Integer.MAX_VALUE));

        HecatonInstance server = factory.newHecatonInstance(config);

        CachingProvider provider = createServerCachingProvider(server);
        CacheManager serverCacheManager = provider.getCacheManager();

        serverCache1 = serverCacheManager.createCache(CACHE_1_NAME, cacheConfig);
        serverCache2 = serverCacheManager.createCache(CACHE_2_NAME, cacheConfig);
        serverCache3 = serverCacheManager.createCache(CACHE_3_NAME, cacheConfig);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void test_reconciliation_does_not_cause_premature_removal() {
        int total = 100;
        for (int i = 0; i < total; i++) {
            serverCache1.put(i, i);
        }

        clientCache1 = createCacheFromNewClient(CACHE_1_NAME);

        for (int i = 0; i < total; i++) {
            clientCache1.get(i);
        }

        NearCacheStats nearCacheStats = getNearCacheStatisticsOf(clientCache1);
        assertStats(CACHE_1_NAME, nearCacheStats, total, 0, total);

        sleepSeconds(2 * RECONCILIATION_INTERVAL_SECS);

        for (int i = 0; i < total; i++) {
            clientCache1.get(i);
        }

        assertStats(CACHE_1_NAME, nearCacheStats, total, total, total);
    }

    private static NearCacheStats getNearCacheStatisticsOf(Cache cache) {
        return ((ICache) cache).getLocalCacheStatistics().getNearCacheStatistics();
    }

    @Test
    public void test_reconciliation_does_not_cause_premature_removal_on_other_caches_after_cache_clear() {
        int total = 91;
        for (int i = 0; i < total; i++) {
            serverCache1.put(i, i);
            serverCache2.put(i, i);
            serverCache3.put(i, i);
        }

        clientCache1 = createCacheFromNewClient(CACHE_1_NAME);
        clientCache2 = createCacheFromNewClient(CACHE_2_NAME);
        clientCache3 = createCacheFromNewClient(CACHE_3_NAME);

        for (int i = 0; i < total; i++) {
            clientCache1.get(i);
            clientCache2.get(i);
            clientCache3.get(i);
        }

        assertStats(CACHE_1_NAME, getNearCacheStatisticsOf(clientCache1), total, 0, total);
        assertStats(CACHE_2_NAME, getNearCacheStatisticsOf(clientCache2), total, 0, total);
        assertStats(CACHE_3_NAME, getNearCacheStatisticsOf(clientCache3), total, 0, total);

        // Call map.clear on 1st map
        clientCache1.clear();

        // Sleep a little, hence we can see effect of reconciliation-task.
        sleepSeconds(2 * RECONCILIATION_INTERVAL_SECS);

        // Do subsequent gets on maps.
        // Except for 1st map, other maps should
        // return responses from their near-cache.
        for (int i = 0; i < total; i++) {
            clientCache1.get(i);
            clientCache2.get(i);
            clientCache3.get(i);
        }

        // clientCache1 doesn't cache null values so, at
        // here, it's near cache should be empty for clientCache1.
        assertStats(CACHE_1_NAME, getNearCacheStatisticsOf(clientCache1), 0, 0, 2 * total);
        assertStats(CACHE_2_NAME, getNearCacheStatisticsOf(clientCache2), total, total, total);
        assertStats(CACHE_3_NAME, getNearCacheStatisticsOf(clientCache3), total, total, total);
    }

    private Cache<Integer, Integer> createCacheFromNewClient(String cacheName) {
        HecatonClientProxy client = (HecatonClientProxy) factory.newHecatonClient(clientConfig);
        CachingProvider clientCachingProvider = createClientCachingProvider(client);

        CacheManager cacheManager = clientCachingProvider.getCacheManager();
        Cache<Integer, Integer> cache = cacheManager.createCache(cacheName, cacheConfig);

        assertInstanceOf(NearCachedClientCacheProxy.class, cache);

        return cache;
    }
}
