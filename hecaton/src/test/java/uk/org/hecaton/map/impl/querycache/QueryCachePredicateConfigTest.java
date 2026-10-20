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
import uk.org.hecaton.config.PredicateConfig;
import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.QueryCache;
import uk.org.hecaton.map.impl.querycache.utils.Employee;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.map.impl.querycache.AbstractQueryCacheTestSupport.getMap;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCachePredicateConfigTest extends HecatonTestSupport {

    @Test
    public void test_whenSqlIsSet() {
        String mapName = randomString();
        String cacheName = randomString();

        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(mapName);

        QueryCacheConfig cacheConfig = new QueryCacheConfig(cacheName);
        PredicateConfig predicateConfig = cacheConfig.getPredicateConfig();
        predicateConfig.setSql("id > 10");

        mapConfig.addQueryCacheConfig(cacheConfig);

        HecatonInstance node = createHecatonInstance(config);
        IMap<Integer, Employee> map = getMap(node, mapName);

        for (int i = 0; i < 15; i++) {
            map.put(i, new Employee(i));
        }

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName);

        assertEquals(4, cache.size());
    }

    @Test
    public void test_whenClassNameIsSet() {
        String mapName = randomString();
        String cacheName = randomString();

        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(mapName);

        QueryCacheConfig cacheConfig = new QueryCacheConfig(cacheName);
        PredicateConfig predicateConfig = cacheConfig.getPredicateConfig();
        predicateConfig.setClassName("uk.org.hecaton.map.impl.querycache.TestPredicate");

        mapConfig.addQueryCacheConfig(cacheConfig);

        HecatonInstance node = createHecatonInstance(config);
        IMap<Integer, Employee> map = getMap(node, mapName);

        for (int i = 0; i < 15; i++) {
            map.put(i, new Employee(i));
        }

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName);

        assertEquals(0, cache.size());
    }
}
