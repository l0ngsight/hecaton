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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientQueryCacheUpdateTest extends HecatonTestSupport {

    private final String mapName = "mapName";
    private final String queryCacheName = "queryCacheName";
    private final TestHecatonFactory factory = new TestHecatonFactory();

    private HecatonInstance client;

    @Before
    public void setUp() {
        factory.newHecatonInstance(getConfig());

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        queryCacheConfig.getPredicateConfig().setImplementation(Predicates.sql("id=1"));

        ClientConfig config = new ClientConfig();
        config.addQueryCacheConfig(mapName, queryCacheConfig);

        client = factory.newHecatonClient(config);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void query_cache_gets_latest_updated_value_when_map_is_updated_via_set_method() {
        IMap<Integer, IdWrapper> clientMap = client.getMap(mapName);
        final QueryCache<Integer, IdWrapper> queryCache = clientMap.getQueryCache(queryCacheName);

        int id = 1;
        for (int value = 0; value < 10; value++) {
            clientMap.set(id, new IdWrapper(id, value));
        }

        assertTrueEventually(() -> {
            IdWrapper idWrapper = queryCache.get(1);
            assertNotNull(idWrapper);
            assertEquals(9, idWrapper.value);
        });
    }

    public static class IdWrapper implements Serializable {

        public int id;
        public int value;

        public IdWrapper(int id, int value) {
            this.id = id;
            this.value = value;
        }
    }
}
