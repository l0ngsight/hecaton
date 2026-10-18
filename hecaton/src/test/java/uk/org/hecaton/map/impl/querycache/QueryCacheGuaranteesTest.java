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
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.QueryCache;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.map.impl.querycache.AbstractQueryCacheTestSupport.getMap;
import static org.junit.Assert.assertEquals;

/**
 * This test does not test an actual event arrival guarantee between the subscriber and publisher.
 * The guarantee should be deemed in the context of {@link QueryCache} design.
 * Because the design relies on a fire&forget eventing system.
 * So this test should be thought under query-cache-context-guarantees and it is fragile.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheGuaranteesTest extends HecatonTestSupport {

    @Test
    public void continuesToReceiveEvents_afterNodeJoins() {
        String mapName = randomString();
        String queryCacheName = randomString();
        TestHecatonInstanceFactory instanceFactory = createHecatonInstanceFactory(3);
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "271");

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        queryCacheConfig.setBatchSize(100);
        queryCacheConfig.setDelaySeconds(3);

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.addQueryCacheConfig(queryCacheConfig);

        HecatonInstance node = instanceFactory.newHecatonInstance(config);
        IMap<Integer, Integer> map = getMap(node, mapName);

        final QueryCache<Integer, Integer> queryCache = map.getQueryCache(queryCacheName, Predicates.sql("this > 20"), true);

        for (int i = 0; i < 30; i++) {
            map.put(i, i);
        }

        HecatonInstance node2 = instanceFactory.newHecatonInstance(config);
        IMap<Integer, Integer> map2 = node2.getMap(mapName);

        for (int i = 100; i < 120; i++) {
            map2.put(i, i);
        }

        HecatonInstance node3 = instanceFactory.newHecatonInstance(config);
        IMap<Integer, Integer> map3 = node3.getMap(mapName);

        for (int i = 150; i < 157; i++) {
            map3.put(i, i);
        }

        assertTrueEventually(() -> assertEquals(36, queryCache.size()));
    }

    @Test
    public void continuesToReceiveEvents_afterNodeShutdown() {
        String mapName = randomString();
        String queryCacheName = randomString();
        TestHecatonInstanceFactory instanceFactory = createHecatonInstanceFactory(3);
        Config config = new Config();

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        queryCacheConfig.setBatchSize(100);
        queryCacheConfig.setDelaySeconds(6);

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.addQueryCacheConfig(queryCacheConfig);

        HecatonInstance node = instanceFactory.newHecatonInstance(config);
        IMap<Integer, Integer> map = node.getMap(mapName);

        final QueryCache<Integer, Integer> queryCache = map.getQueryCache(queryCacheName, Predicates.sql("this > 20"), true);

        for (int i = 0; i < 30; i++) {
            map.put(i, i);
        }

        HecatonInstance node2 = instanceFactory.newHecatonInstance(config);
        IMap<Integer, Integer> map2 = node2.getMap(mapName);

        for (int i = 200; i < 220; i++) {
            map2.put(i, i);
        }

        HecatonInstance node3 = instanceFactory.newHecatonInstance(config);
        IMap<Integer, Integer> map3 = node3.getMap(mapName);

        for (int i = 350; i < 357; i++) {
            map3.put(i, i);
        }

        node3.shutdown();

        for (int i = 220; i < 227; i++) {
            map2.put(i, i);
        }

        assertTrueEventually(() -> assertEquals(43, queryCache.size()));
    }
}
