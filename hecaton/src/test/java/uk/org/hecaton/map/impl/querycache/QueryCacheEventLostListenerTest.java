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
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.QueryCache;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.querycache.subscriber.TestSubscriberContext;
import uk.org.hecaton.map.listener.EventLostListener;
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

import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.map.impl.querycache.AbstractQueryCacheTestSupport.getMap;
import static uk.org.hecaton.test.Accessors.getNode;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheEventLostListenerTest extends HecatonTestSupport {

    @Test
    public void testListenerNotified_uponEventLoss() {
        String mapName = randomString();
        String queryCacheName = randomString();
        TestHecatonInstanceFactory instanceFactory = createHecatonInstanceFactory(3);
        Config config = new Config();

        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        queryCacheConfig.setBatchSize(1111);
        queryCacheConfig.setDelaySeconds(3);

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.addQueryCacheConfig(queryCacheConfig);
        mapConfig.setBackupCount(0);

        HecatonInstance node = instanceFactory.newHecatonInstance(config);
        HecatonInstance node2 = instanceFactory.newHecatonInstance(config);
        setTestSequencer(node, 9);
        setTestSequencer(node2, 9);

        IMap<Integer, Integer> map = getMap(node, mapName);
        node2.getMap(mapName);

        // set test sequencer to all nodes
        int count = 30;

        // expecting one lost event per partition
        final CountDownLatch lossCount = new CountDownLatch(1);
        final QueryCache<Integer, Integer> queryCache = map.getQueryCache(queryCacheName, Predicates.sql("this > 20"), true);
        queryCache.addEntryListener((EventLostListener) event -> lossCount.countDown(), false);

        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        assertOpenEventually(lossCount);
    }

    private void setTestSequencer(HecatonInstance instance, int eventCount) {
        Node node = getNode(instance);
        MapService service = node.getNodeEngine().getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        queryCacheContext.setSubscriberContext(new TestSubscriberContext(queryCacheContext, eventCount, true));
    }
}
