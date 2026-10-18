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
import uk.org.hecaton.client.map.impl.querycache.subscriber.TestClientSubscriberContext;
import uk.org.hecaton.client.impl.proxy.ClientMapProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.QueryCache;
import uk.org.hecaton.map.impl.querycache.QueryCacheContext;
import uk.org.hecaton.map.listener.EventLostListener;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_COUNT;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientQueryCacheRecoveryUponEventLossTest extends HecatonTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testForceConsistency() {
        String mapName = randomMapName("map");
        String queryCacheName = randomMapName("cache");
        Config config = getConfig();

        config.setProperty(PARTITION_COUNT.getName(), "1");
        factory.newHecatonInstance(config);

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        queryCacheConfig.setBatchSize(1111);
        queryCacheConfig.setDelaySeconds(3);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addQueryCacheConfig(mapName, queryCacheConfig);

        HecatonInstance client = factory.newHecatonClient(clientConfig);

        IMap<Integer, Integer> map = client.getMap(mapName);
        // set test sequencer to subscriber
        int count = 30;
        setTestSequencer(map, 9);

        final QueryCache queryCache = map.getQueryCache(queryCacheName, Predicates.sql("this > 20"), true);
        queryCache.addEntryListener((EventLostListener) event -> queryCache.tryRecover(), false);


        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        assertTrueEventually(() -> assertEquals(9, queryCache.size()));
    }

    private void setTestSequencer(IMap map, int eventCount) {
        ClientMapProxy proxy = (ClientMapProxy) map;
        QueryCacheContext queryCacheContext = proxy.getQueryCacheContext();
        queryCacheContext.setSubscriberContext(new TestClientSubscriberContext(queryCacheContext, eventCount, true));
    }
}
