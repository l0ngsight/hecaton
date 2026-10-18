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
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.QueryCache;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheCreateDestroyTest extends HecatonTestSupport {

    private final TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test
    public void create_after_destroy_gives_fresh_query_cache_instance() {
        final String mapName = "someMap";
        final String queryCacheName = "testCache";

        HecatonInstance server = factory.newHecatonInstance(newConfigWithQueryCache(mapName, queryCacheName));
        server.getMap(mapName);

        IMap map = server.getMap(mapName);
        QueryCache queryCache = map.getQueryCache(queryCacheName);

        queryCache.destroy();
        QueryCache newQueryCache = map.getQueryCache(queryCacheName);

        assertNotSame(queryCache, newQueryCache);
    }

    @Test
    public void recreated_queryCache_gets_updates_from_restarted_server() {
        final String mapName = "someMap";
        final String queryCacheName = "testCache";

        // start a server
        HecatonInstance server = factory.newHecatonInstance();
        server.getMap(mapName);

        // start a serverWithQueryCache
        HecatonInstance serverWithQueryCache = factory.newHecatonInstance(newConfigWithQueryCache(mapName, queryCacheName));

        // create query-cache
        IMap map = serverWithQueryCache.getMap(mapName);
        QueryCache queryCache = map.getQueryCache(queryCacheName);

        // shutdown other member, at this point only serverWithQueryCache is alive
        server.shutdown();

        // start new server to emulate server re-start
        HecatonInstance newServer = factory.newHecatonInstance();
        IMap newServerMap = newServer.getMap(mapName);

        // populate new map from newServer
        for (int i = 0; i < 1000; i++) {
            newServerMap.put(i, i);
        }

        // destroy query-cache from serverWithQueryCache
        queryCache.destroy();

        // new query-cache should have all updates from server side
        queryCache = map.getQueryCache(queryCacheName);
        int queryCacheSize = queryCache.size();

        assertEquals(1000, queryCacheSize);
    }

    @Test
    public void tryRecover_fails_after_destroy() {
        final String mapName = "someMap";
        final String queryCacheName = "testCache";

        HecatonInstance server = factory.newHecatonInstance(newConfigWithQueryCache(mapName, queryCacheName));
        server.getMap(mapName);

        IMap map = server.getMap(mapName);
        QueryCache queryCache = map.getQueryCache(queryCacheName);

        queryCache.destroy();

        assertFalse(queryCache.tryRecover());
    }


    private static Config newConfigWithQueryCache(String mapName, String queryCacheName) {
        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        queryCacheConfig.getPredicateConfig().setImplementation(Predicates.alwaysTrue());
        queryCacheConfig.setPopulate(true);

        Config config = new Config();
        config.getMapConfig(mapName).addQueryCacheConfig(queryCacheConfig);

        return config;
    }
}
