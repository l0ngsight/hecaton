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

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.querycache.QueryCacheContext;
import uk.org.hecaton.map.listener.EntryAddedListener;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.eventservice.impl.EventServiceImpl;
import uk.org.hecaton.spi.impl.eventservice.impl.EventServiceSegment;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NodeQueryCacheEventServiceTest extends HecatonTestSupport {

    @Test
    public void no_left_over_listener_after_concurrent_addition_and_removal_on_same_queryCache() throws InterruptedException {
        final String mapName = "test";
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        final HecatonInstance node = factory.newHecatonInstance();

        SubscriberContext subscriberContext = getSubscriberContext(node);
        final NodeQueryCacheEventService nodeQueryCacheEventService
                = (NodeQueryCacheEventService) subscriberContext.getEventService();

        final AtomicBoolean stop = new AtomicBoolean();
        ArrayList<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Thread thread = new Thread(() -> {
                while (!stop.get()) {
                    nodeQueryCacheEventService.addListener(mapName, "a", (EntryAddedListener<Object, Object>) event -> {

                    });

                    nodeQueryCacheEventService.removeAllListeners(mapName, "a");
                }
            });
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.start();
        }

        sleepSeconds(5);
        stop.set(true);

        for (Thread thread : threads) {
            thread.join();
        }

        assertNoUserListenerLeft(node);
    }

    private static SubscriberContext getSubscriberContext(HecatonInstance node) {
        MapService mapService = getNodeEngineImpl(node).getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        return queryCacheContext.getSubscriberContext();
    }

    private static void assertNoUserListenerLeft(HecatonInstance node) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(node);
        EventServiceImpl eventServiceImpl = (EventServiceImpl) nodeEngineImpl.getEventService();
        EventServiceSegment segment = eventServiceImpl.getSegment(MapService.SERVICE_NAME, false);
        ConcurrentMap registrations = segment.getRegistrations();
        ConcurrentMap registrationIdMap = segment.getRegistrationIdMap();

        assertTrue(registrations.toString(), registrations.isEmpty());
        assertTrue(registrationIdMap.toString(), registrationIdMap.isEmpty());
    }
}
