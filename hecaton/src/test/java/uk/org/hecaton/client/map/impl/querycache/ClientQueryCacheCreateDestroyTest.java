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
import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.QueryCache;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientQueryCacheCreateDestroyTest extends HecatonTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void create_after_destroy_gives_fresh_query_cache_instance() {
        final String mapName = "someMap";
        final String queryCacheName = "testCache";

        HecatonInstance server = factory.newHecatonInstance();
        HecatonInstance client = factory.newHecatonClient(newClientConfigWithQueryCache(mapName, queryCacheName));
        // create client-side query-cache
        IMap clientMap = client.getMap(mapName);

        QueryCache clientQueryCache = clientMap.getQueryCache(queryCacheName);

        clientQueryCache.destroy();

        QueryCache newQueryCache = clientMap.getQueryCache(queryCacheName);

        assertNotSame(clientQueryCache, newQueryCache);
    }

    @Test
    public void recreated_queryCache_gets_updates_from_restarted_server() {
        final String mapName = "someMap";
        final String queryCacheName = "testCache";

        // start server
        HecatonInstance server = factory.newHecatonInstance();
        server.getMap(mapName);

        // start client with query-cache config
        HecatonInstance client = factory.newHecatonClient(newClientConfigWithQueryCache(mapName, queryCacheName));

        // create client-side query-cache
        IMap clientMap = client.getMap(mapName);
        QueryCache clientQueryCache = clientMap.getQueryCache(queryCacheName);

        // shutdown all members, at this point only client is alive
        factory.shutdownAllMembers();

        // start new server to emulate server re-start
        HecatonInstance newServer = factory.newHecatonInstance();
        IMap newServerMap = newServer.getMap(mapName);

        // populate new map on server
        for (int i = 0; i < 1000; i++) {
            newServerMap.put(i, i);
        }

        // destroy client query-cache from client-side to re-create it,
        // otherwise it stays tied with previous server
        clientQueryCache.destroy();

        // new client-side query-cache should have all updates from server side
        clientQueryCache = clientMap.getQueryCache(queryCacheName);
        int queryCacheSize = clientQueryCache.size();
        assertEquals(1000, queryCacheSize);
    }

    private static ClientConfig newClientConfigWithQueryCache(String mapName, String queryCacheName) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        queryCacheConfig.getPredicateConfig().setImplementation(Predicates.alwaysTrue());
        queryCacheConfig.setPopulate(true);

        clientConfig.addQueryCacheConfig(mapName, queryCacheConfig);

        return clientConfig;
    }

    @Test
    public void multiple_getQueryCache_calls_returns_same_query_cache_instance() {
        final String mapName = "someMap";
        final String queryCacheName = "testCache";

        HecatonInstance server = factory.newHecatonInstance();
        HecatonInstance client = factory.newHecatonClient(newClientConfigWithQueryCache(mapName, queryCacheName));
        // create client-side query-cache
        IMap clientMap = client.getMap(mapName);

        QueryCache clientQueryCache1 = clientMap.getQueryCache(queryCacheName);
        QueryCache clientQueryCache2 = clientMap.getQueryCache(queryCacheName);

        assertSame(clientQueryCache1, clientQueryCache2);
    }
}
