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

package uk.org.hecaton.map.impl.querycache.subscriber;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.QueryCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.QueryCache;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.querycache.QueryCacheContext;
import uk.org.hecaton.map.impl.querycache.accumulator.Accumulator;
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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.map.impl.querycache.AbstractQueryCacheTestSupport.getMap;
import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheRecoveryUponEventLossTest extends HecatonTestSupport {

    @Test
    public void testForceConsistency() {
        TestHecatonInstanceFactory instanceFactory = createHecatonInstanceFactory(3);

        final String mapName = randomString();
        String queryCacheName = randomString();

        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        queryCacheConfig.setBatchSize(1111);
        queryCacheConfig.setDelaySeconds(3);

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.addQueryCacheConfig(queryCacheConfig);
        mapConfig.setBackupCount(0);

        final HecatonInstance node = instanceFactory.newHecatonInstance(config);
        HecatonInstance node2 = instanceFactory.newHecatonInstance(config);
        setTestSequencer(node, 9);
        setTestSequencer(node2, 9);

        IMap<Integer, Integer> map = getMap(node, mapName);
        node2.getMap(mapName);

        final CountDownLatch waitEventLossNotification = new CountDownLatch(1);
        final QueryCache queryCache = map.getQueryCache(queryCacheName, Predicates.sql("this > 20"), true);
        queryCache.addEntryListener((EventLostListener) event -> {
            queryCache.tryRecover();

            waitEventLossNotification.countDown();
        }, false);

        int count = 30;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        assertOpenEventually(waitEventLossNotification);

        assertTrueEventually(() -> assertEquals(9, queryCache.size()));

        // re-put entries and check if broken-sequences holder map will be empty
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        assertTrueEventually(() -> {
            Map<Integer, Long> brokenSequences = getBrokenSequences(node, mapName, queryCache);
            assertTrue("After recovery, there should be no broken sequences left",
                    brokenSequences.isEmpty());
        });
    }

    private void setTestSequencer(HecatonInstance instance, int eventCount) {
        Node node = getNode(instance);
        MapService service = node.getNodeEngine().getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        queryCacheContext.setSubscriberContext(new TestSubscriberContext(queryCacheContext, eventCount, true));
    }

    private Map<Integer, Long> getBrokenSequences(HecatonInstance instance, String mapName, QueryCache queryCache) {
        Node node = getNode(instance);
        MapService service = node.getNodeEngine().getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        QueryCacheContext context = mapServiceContext.getQueryCacheContext();

        SubscriberContext subscriberContext = context.getSubscriberContext();
        MapSubscriberRegistry mapSubscriberRegistry = subscriberContext.getMapSubscriberRegistry();

        SubscriberRegistry subscriberRegistry = mapSubscriberRegistry.getOrNull(mapName);
        if (subscriberRegistry == null) {
            return Collections.emptyMap();
        }

        String cacheId = ((InternalQueryCache) queryCache).getCacheId();
        Accumulator accumulator = subscriberRegistry.getOrNull(cacheId);
        if (accumulator == null) {
            return Collections.emptyMap();
        }

        SubscriberAccumulator subscriberAccumulator = (SubscriberAccumulator) accumulator;
        return subscriberAccumulator.getBrokenSequences();
    }
}
