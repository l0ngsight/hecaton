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
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.EvictionPolicy;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.EventLostEvent;
import uk.org.hecaton.map.QueryCache;
import uk.org.hecaton.map.listener.EventLostListener;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.AssertTask;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class QueryCacheSimpleStressTest extends HecatonTestSupport {

    private static final Predicate<Integer, Integer> TRUE_PREDICATE = Predicates.alwaysTrue();

    private final String mapName = randomString();
    private final String cacheName = randomString();
    private final Config config = new Config();
    private final int numberOfElementsToPut = 10000;

    @Before
    public void setUp() {
        EvictionConfig evictionConfig = new EvictionConfig();
        evictionConfig.setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);
        evictionConfig.setSize(Integer.MAX_VALUE);
        evictionConfig.setEvictionPolicy(EvictionPolicy.LRU);

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
        queryCacheConfig
                .setBufferSize(30)
                .setDelaySeconds(2)
                .setBatchSize(2)
                .setPopulate(true)
                .getPredicateConfig().setImplementation(Predicates.alwaysTrue());
        queryCacheConfig.setEvictionConfig(evictionConfig);

        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.addQueryCacheConfig(queryCacheConfig);
        config.addMapConfig(mapConfig);
    }

    @Test
    public void testStress() throws Exception {
        final IMap<Integer, Integer> map = getMap();

        Runnable runnable = () -> {
            for (int i = 0; i < numberOfElementsToPut; i++) {
                map.put(i, i);
            }
        };

        Thread thread = new Thread(runnable);
        thread.start();

        final QueryCache<Integer, Integer> queryCache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        queryCache.addEntryListener(new EventLostListener() {
            @Override
            public void eventLost(EventLostEvent event) {
                queryCache.tryRecover();
            }
        }, true);

        thread.join();

        assertQueryCacheSizeEventually(numberOfElementsToPut, queryCache);
    }

    private <K, V> IMap<K, V> getMap() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        HecatonInstance[] instances = factory.newInstances(config);
        HecatonInstance node = instances[0];
        return AbstractQueryCacheTestSupport.getMap(node, mapName);
    }

    private void assertQueryCacheSizeEventually(final int expected, final QueryCache queryCache) {
        AssertTask task = () -> assertEquals(expected, queryCache.size());

        assertTrueEventually(task, 20);
    }
}
