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

import uk.org.hecaton.client.map.impl.querycache.subscriber.TestClientSubscriberContext;
import uk.org.hecaton.client.impl.proxy.ClientMapProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.EventLostEvent;
import uk.org.hecaton.map.QueryCache;
import uk.org.hecaton.map.impl.querycache.QueryCacheContext;
import uk.org.hecaton.map.listener.EventLostListener;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientQueryCacheEventLostListenerTest extends HecatonTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();
    private HecatonInstance node;

    @Before
    public void setUp() {
        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");

        node = factory.newHecatonInstance(config);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testListenerNotified_onEventLoss() {
        int count = 30;
        String mapName = randomString();
        String queryCacheName = randomString();

        IMap<Integer, Integer> mapNode = node.getMap(mapName);

        HecatonInstance client = factory.newHecatonClient();

        IMap<Integer, Integer> mapClient = client.getMap(mapName);
        setTestSequencer(mapClient, 9);

        // expecting one lost event publication per partition.
        final CountDownLatch lostEventCount = new CountDownLatch(1);
        QueryCache queryCache = mapClient.getQueryCache(queryCacheName, Predicates.sql("this > 20"), true);
        queryCache.addEntryListener(new EventLostListener() {
            @Override
            public void eventLost(EventLostEvent event) {
                lostEventCount.countDown();
            }
        }, false);


        for (int i = 0; i < count; i++) {
            mapNode.put(i, i);
        }

        assertOpenEventually(lostEventCount);
    }

    private void setTestSequencer(IMap map, int eventCount) {
        ClientMapProxy proxy = (ClientMapProxy) map;
        QueryCacheContext queryCacheContext = proxy.getQueryCacheContext();
        queryCacheContext.setSubscriberContext(new TestClientSubscriberContext(queryCacheContext, eventCount, true));
    }
}
