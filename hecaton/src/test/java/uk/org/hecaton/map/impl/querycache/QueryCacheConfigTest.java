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
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.map.impl.querycache.AbstractQueryCacheTestSupport.getMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheConfigTest extends HecatonTestSupport {

    @Test
    public void testDifferentQueryCacheInstancesObtained_whenIMapConfiguredWithWildCard() {
        QueryCacheConfig cacheConfig = new QueryCacheConfig();
        cacheConfig.setName("cache");
        cacheConfig.getPredicateConfig().setSql("__key > 10");

        HecatonInstance hecatonInstance = createInstanceWithQueryCacheConfig("test*", cacheConfig);
        IMap<Integer, Integer> map1 = getMap(hecatonInstance, "test1");
        IMap<Integer, Integer> map2 = getMap(hecatonInstance, "test2");

        QueryCache<Integer, Integer> queryCache1 = map1.getQueryCache("cache");
        QueryCache<Integer, Integer> queryCache2 = map2.getQueryCache("cache");

        for (int i = 0; i < 20; i++) {
            map1.put(i, i);
        }

        for (int i = 0; i < 30; i++) {
            map2.put(i, i);
        }

        assertQueryCacheSizeEventually(9, queryCache1);
        assertQueryCacheSizeEventually(19, queryCache2);
    }

    @Test
    public void testQueryCacheNameConfiguredWithWildCard() {
        QueryCacheConfig cacheConfig = new QueryCacheConfig();
        String mapNamePrefix = "MyMap";
        String mapNameNameWithWildcard = mapNamePrefix + "*";

        cacheConfig.setName("myCache");
        cacheConfig.getPredicateConfig().setSql("__key > 10");

        HecatonInstance hecatonInstance = createInstanceWithQueryCacheConfig(mapNameNameWithWildcard, cacheConfig);
        IMap<Integer, Integer> map1 = getMap(hecatonInstance, mapNamePrefix + "_1");

        QueryCache<Integer, Integer> queryCache1 = map1.getQueryCache("myCache");
        assertNotNull(queryCache1);
    }

    private void assertQueryCacheSizeEventually(final int expected, final QueryCache cache) {
        assertTrueEventually(() -> assertEquals(expected, cache.size()));
    }

    protected HecatonInstance createInstanceWithQueryCacheConfig(String mapName, QueryCacheConfig queryCacheConfig) {
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(mapName);

        mapConfig.addQueryCacheConfig(queryCacheConfig);

        Config config = new Config();
        config.addMapConfig(mapConfig);

        return createHecatonInstance(config);
    }
}
