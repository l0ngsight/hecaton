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

package uk.org.hecaton.map.impl.querycache;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.QueryCache;
import uk.org.hecaton.map.listener.EntryUpdatedListener;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.map.impl.querycache.AbstractQueryCacheTestSupport.getMap;
import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_COUNT;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheCoalescingTest extends HecatonTestSupport {

    private static final Predicate<Integer, Integer> TRUE_PREDICATE = Predicates.alwaysTrue();

    @Test
    public void testCoalescingModeWorks() {
        String mapName = randomString();
        String cacheName = randomString();

        Config config = getConfig(mapName, cacheName);
        HecatonInstance node = createHecatonInstance(config);
        IMap<Integer, Integer> map = getMap(node, mapName);

        final CountDownLatch updateEventCount = new CountDownLatch(1);
        final QueryCache<Integer, Integer> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        cache.addEntryListener(new EntryUpdatedListener() {
            @Override
            public void entryUpdated(EntryEvent event) {
                updateEventCount.countDown();
            }
        }, false);

        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }

        // update same key to control whether coalescing kicks in.
        for (int i = 0; i < 500; i++) {
            map.put(0, i);
        }

        assertTrueEventually(() -> assertEquals(100, cache.size()));
        assertOpenEventually(updateEventCount);
    }

    private Config getConfig(String mapName, String cacheName) {
        QueryCacheConfig cacheConfig = new QueryCacheConfig(cacheName)
                .setCoalesce(true)
                .setBatchSize(64)
                .setBufferSize(64)
                .setDelaySeconds(3);

        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1");

        config.getMapConfig(mapName)
                .addQueryCacheConfig(cacheConfig);

        return config;
    }
}
