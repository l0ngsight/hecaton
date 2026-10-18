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

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.QueryCache;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.AssertTask;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category(SlowTest.class)
public class ClientQueryCacheSimpleStressTest extends HecatonTestSupport {

    private final String mapName = randomString();
    private final String cacheName = randomString();
    private final ClientConfig config = new ClientConfig();
    private final int numberOfElementsToPut = 10000;
    private HecatonInstance instance;

    @Before
    public void setUp() {
        Hecaton.newHecatonInstance();
        Hecaton.newHecatonInstance();
        Hecaton.newHecatonInstance();

        setQueryCacheConfig();

        instance = HecatonClient.newHecatonClient(config);
    }

    private void setQueryCacheConfig() {
        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);
        queryCacheConfig
                .setBufferSize(30)
                .setDelaySeconds(2)
                .setBatchSize(2)
                .setPopulate(true)
                .getPredicateConfig().setImplementation(Predicates.alwaysTrue());

        config.addQueryCacheConfig(mapName, queryCacheConfig);
    }

    @After
    public void tearDown() {
        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
    }

    @Test
    public void testStress() throws Exception {
        final IMap<Integer, Integer> map = instance.getMap(mapName);

        Runnable runnable = () -> {
            for (int i = 0; i < numberOfElementsToPut; i++) {
                map.put(i, i);
            }
        };

        Thread thread = new Thread(runnable);
        thread.start();

        QueryCache<Integer, Integer> queryCache = map.getQueryCache(cacheName, Predicates.alwaysTrue(), true);

        thread.join();

        assertQueryCacheSizeEventually(numberOfElementsToPut, queryCache);
    }

    private void assertQueryCacheSizeEventually(final int expected, final QueryCache queryCache) {
        AssertTask task = () -> assertEquals(expected, queryCache.size());

        assertTrueEventually(task);
    }
}
