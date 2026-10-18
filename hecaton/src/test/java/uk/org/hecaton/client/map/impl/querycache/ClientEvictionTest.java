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

package uk.org.hecaton.client.map.impl.querycache;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.EvictionPolicy;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.QueryCache;
import uk.org.hecaton.map.listener.EntryEvictedListener;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientEvictionTest extends HecatonTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @Before
    public void setUp() {
        factory.newHecatonInstance();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testMaxSizeEvictionWorks() {
        final int maxSize = 1000;
        final int populationCount = 5000;

        String mapName = randomString();
        String cacheName = randomString();

        QueryCacheConfig cacheConfig = new QueryCacheConfig(cacheName);
        cacheConfig.getEvictionConfig()
                .setSize(maxSize)
                .setEvictionPolicy(EvictionPolicy.LFU)
                .setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addQueryCacheConfig(mapName, cacheConfig);

        HecatonInstance client = factory.newHecatonClient(clientConfig);
        IMap<Integer, Integer> map = client.getMap(mapName);

        // expecting at least populationCount - maxSize + 10 evicted entries according to max size.
        // 10 states an error margin since eviction does not sweep precise number of entries.
        int margin = 10;
        final CountDownLatch evictedCount = new CountDownLatch(populationCount - maxSize - margin);
        final QueryCache<Integer, Integer> cache = map.getQueryCache(cacheName, Predicates.alwaysTrue(), true);
        UUID listener = cache.addEntryListener((EntryEvictedListener<Integer, Integer>) event -> evictedCount.countDown(), false);

        for (int i = 0; i < populationCount; i++) {
            map.put(i, i);
        }

        assertOpenEventually(evictedCount);
        assertQueryCacheEvicted(maxSize, margin, cache);
        assertTrue(cache.removeEntryListener(listener));
    }

    private void assertQueryCacheEvicted(int maxSize, int margin, QueryCache<Integer, Integer> cache) {
        int size = cache.size();
        assertTrue("cache size = " + size + ", should be smaller than max size = " + maxSize, size < maxSize + margin);
    }
}
