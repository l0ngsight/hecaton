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

import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.impl.proxy.ClientMapProxy;
import uk.org.hecaton.client.impl.spi.ClientContext;
import uk.org.hecaton.client.impl.spi.ProxyManager;
import uk.org.hecaton.client.map.impl.querycache.subscriber.ClientQueryCacheEventService;
import uk.org.hecaton.client.map.impl.querycache.subscriber.QueryCacheToListenerMapper;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.QueryCache;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.querycache.QueryCacheContext;
import uk.org.hecaton.map.impl.querycache.QueryCacheEventService;
import uk.org.hecaton.map.impl.querycache.accumulator.DefaultAccumulatorInfoSupplier;
import uk.org.hecaton.map.impl.querycache.publisher.MapListenerRegistry;
import uk.org.hecaton.map.impl.querycache.publisher.MapPublisherRegistry;
import uk.org.hecaton.map.impl.querycache.publisher.PartitionAccumulatorRegistry;
import uk.org.hecaton.map.impl.querycache.publisher.PublisherContext;
import uk.org.hecaton.map.impl.querycache.publisher.PublisherRegistry;
import uk.org.hecaton.map.impl.querycache.publisher.QueryCacheListenerRegistry;
import uk.org.hecaton.map.impl.querycache.subscriber.QueryCacheEndToEndProvider;
import uk.org.hecaton.map.impl.querycache.subscriber.QueryCacheFactory;
import uk.org.hecaton.map.impl.querycache.subscriber.SubscriberContext;
import uk.org.hecaton.map.listener.EntryAddedListener;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.eventservice.impl.EventServiceImpl;
import uk.org.hecaton.spi.impl.eventservice.impl.EventServiceSegment;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.internal.util.RandomPicker.getInt;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientQueryCacheMemoryLeakTest extends HecatonTestSupport {

    private static final int STRESS_TEST_RUN_SECONDS = 3;
    private static final int STRESS_TEST_THREAD_COUNT = 4;

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void stress_user_listener_removal_upon_query_cache_destroy() throws InterruptedException {
        final String[] mapNames = new String[]{"mapA", "mapB", "mapC", "mapD"};

        Config config = getConfig();
        final HecatonInstance node1 = factory.newHecatonInstance(config);
        HecatonInstance node2 = factory.newHecatonInstance(config);
        HecatonInstance node3 = factory.newHecatonInstance(config);

        final HecatonInstance client = factory.newHecatonClient();

        final AtomicBoolean stop = new AtomicBoolean();
        ArrayList<Thread> threads = new ArrayList<>();
        for (int i = 0; i < STRESS_TEST_THREAD_COUNT; i++) {
            Thread thread = new Thread(() -> {
                while (!stop.get()) {
                    String name = mapNames[getInt(0, 4)];
                    final IMap<Integer, Integer> map = client.getMap(name);
                    int key = getInt(0, Integer.MAX_VALUE);
                    map.put(key, 1);
                    QueryCache queryCache = map.getQueryCache(name, Predicates.alwaysTrue(), true);
                    queryCache.get(key);

                    queryCache.addEntryListener((EntryAddedListener<Integer, Integer>) event -> {

                    }, true);

                    queryCache.destroy();
                    map.destroy();
                }
            });
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.start();
        }

        sleepSeconds(STRESS_TEST_RUN_SECONDS);
        stop.set(true);

        for (Thread thread : threads) {
            thread.join();
        }

        assertNoUserListenerLeft(node1);
        assertNoUserListenerLeft(node2);
        assertNoUserListenerLeft(node3);

        for (String mapName : mapNames) {
            // this is to ensure no user added listener is left on this client side query-cache
            assertNoUserListenerLeft(mapName, client);
        }

        Collection<HecatonInstance> instances = factory.getAllHecatonInstances();
        for (HecatonInstance instance : instances) {
            // this is to ensure no publisher side resource is left on server side event service
            assertServerSideEventServiceCleared(instance);
        }
    }

    @Test
    public void event_service_is_empty_after_queryCache_destroy() {
        String mapName = "test";

        HecatonInstance node1 = factory.newHecatonInstance();
        HecatonInstance node2 = factory.newHecatonInstance();

        HecatonInstance client = factory.newHecatonClient();

        IMap<Integer, Integer> map = client.getMap(mapName);
        QueryCache queryCache = map.getQueryCache(mapName, Predicates.alwaysTrue(), true);

        queryCache.destroy();

        assertNoUserListenerLeft(node1);
        assertNoUserListenerLeft(node2);
    }

    @Test
    public void event_service_is_empty_after_queryCache_concurrent_destroy() throws InterruptedException {
        final HecatonInstance node1 = factory.newHecatonInstance();
        HecatonInstance node2 = factory.newHecatonInstance();

        String mapName = "test";
        HecatonInstance client = factory.newHecatonClient();
        final IMap<Integer, Integer> map = client.getMap(mapName);
        populateMap(map);

        final AtomicBoolean stop = new AtomicBoolean();
        ArrayList<Thread> threads = new ArrayList<>();
        for (int i = 0; i < STRESS_TEST_THREAD_COUNT; i++) {
            Thread thread = new Thread(() -> {
                while (!stop.get()) {
                    QueryCache queryCache = map.getQueryCache("a", Predicates.alwaysTrue(), true);

                    queryCache.addEntryListener((EntryAddedListener<Integer, Integer>) event -> {

                    }, true);

                    queryCache.destroy();
                }
            });
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.start();
        }

        sleepSeconds(STRESS_TEST_RUN_SECONDS);
        stop.set(true);

        for (Thread thread : threads) {
            thread.join();
        }

        assertNoUserListenerLeft(node1);
        assertNoUserListenerLeft(node2);
        assertNoUserListenerLeft(mapName, client);
    }

    @Test
    public void removes_internal_query_caches_upon_map_destroy() {
        factory.newHecatonInstance();
        HecatonInstance client = factory.newHecatonClient();

        String mapName = "test";
        IMap<Integer, Integer> map = client.getMap(mapName);

        populateMap(map);

        for (int j = 0; j < 10; j++) {
            map.getQueryCache(j + "-test-QC", Predicates.alwaysTrue(), true);
        }

        map.destroy();

        ClientQueryCacheContext queryCacheContext = ((ClientMapProxy) map).getQueryCacheContext();
        SubscriberContext subscriberContext = queryCacheContext.getSubscriberContext();
        QueryCacheEndToEndProvider provider = subscriberContext.getEndToEndQueryCacheProvider();
        QueryCacheFactory queryCacheFactory = subscriberContext.getQueryCacheFactory();

        assertEquals(0, provider.getQueryCacheCount(mapName));
        assertEquals(0, queryCacheFactory.getQueryCacheCount());
    }

    @Test
    public void no_query_cache_left_after_creating_and_destroying_same_map_concurrently() throws Exception {
        final HecatonInstance node = factory.newHecatonInstance();
        final HecatonInstance client = factory.newHecatonClient();
        final String mapName = "test";

        ExecutorService pool = Executors.newFixedThreadPool(STRESS_TEST_THREAD_COUNT);
        final AtomicBoolean stop = new AtomicBoolean();

        for (int i = 0; i < 1000; i++) {
            Runnable runnable = () -> {
                while (!stop.get()) {
                    IMap<Integer, Integer> map = client.getMap(mapName);
                    try {
                        populateMap(map);
                        for (int j = 0; j < 10; j++) {
                            map.getQueryCache(j + "-test-QC", Predicates.alwaysTrue(), true);
                        }
                    } finally {
                        map.destroy();
                    }
                }
            };
            pool.submit(runnable);
        }

        sleepSeconds(STRESS_TEST_RUN_SECONDS);
        stop.set(true);

        pool.shutdown();
        pool.awaitTermination(120, TimeUnit.SECONDS);

        SubscriberContext subscriberContext = getSubscriberContext(client, mapName);
        final QueryCacheEndToEndProvider provider = subscriberContext.getEndToEndQueryCacheProvider();
        final QueryCacheFactory queryCacheFactory = subscriberContext.getQueryCacheFactory();

        assertTrueEventually(() -> assertEquals(0, provider.getQueryCacheCount(mapName)));

        assertTrueEventually(() -> assertEquals(0, queryCacheFactory.getQueryCacheCount()));

        assertNoListenerLeftOnEventService(node);
        assertNoRegisteredListenerLeft(node, mapName);
        assertNoAccumulatorInfoSupplierLeft(node, mapName);
        assertNoPartitionAccumulatorRegistryLeft(node, mapName);
    }

    private static void assertNoAccumulatorInfoSupplierLeft(HecatonInstance node, String mapName) {
        PublisherContext publisherContext = getPublisherContext(node);
        DefaultAccumulatorInfoSupplier accumulatorInfoSupplier
                = (DefaultAccumulatorInfoSupplier) publisherContext.getAccumulatorInfoSupplier();
        int accumulatorInfoCountOfMap = accumulatorInfoSupplier.accumulatorInfoCountOfMap(mapName);
        assertEquals(0, accumulatorInfoCountOfMap);
    }

    private static void assertNoRegisteredListenerLeft(HecatonInstance node, String mapName) {
        PublisherContext publisherContext = getPublisherContext(node);
        MapListenerRegistry mapListenerRegistry = publisherContext.getMapListenerRegistry();
        QueryCacheListenerRegistry registry = mapListenerRegistry.getOrNull(mapName);
        if (registry != null) {
            Map<String, UUID> registeredListeners = registry.getAll();
            assertTrue(registeredListeners.isEmpty());
        }
    }

    private static void assertNoPartitionAccumulatorRegistryLeft(HecatonInstance node, String mapName) {
        PublisherContext publisherContext = getPublisherContext(node);
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        PublisherRegistry registry = mapPublisherRegistry.getOrCreate(mapName);
        if (registry == null) {
            return;
        }

        Map<String, PartitionAccumulatorRegistry> accumulatorRegistryMap = registry.getAll();
        assertTrue(accumulatorRegistryMap.isEmpty());
    }

    private static void assertNoListenerLeftOnEventService(HecatonInstance node) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(node);
        EventServiceImpl eventService = ((EventServiceImpl) nodeEngineImpl.getEventService());
        EventServiceSegment segment = eventService.getSegment(MapService.SERVICE_NAME, false);
        ConcurrentMap registrationIdMap = segment.getRegistrationIdMap();
        assertEquals(registrationIdMap.toString(), 0, registrationIdMap.size());
    }

    private static void populateMap(IMap<Integer, Integer> map) {
        for (int i = 0; i < 10; i++) {
            map.put(i, i);
        }
    }

    private static SubscriberContext getSubscriberContext(HecatonInstance client, String mapName) {
        final IMap<Integer, Integer> map = client.getMap(mapName);
        return ((ClientMapProxy) map).getQueryCacheContext().getSubscriberContext();
    }

    private static PublisherContext getPublisherContext(HecatonInstance node) {
        MapService mapService = getNodeEngineImpl(node).getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        return queryCacheContext.getPublisherContext();
    }

    private static void assertNoUserListenerLeft(HecatonInstance node) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(node);
        EventServiceImpl eventServiceImpl = (EventServiceImpl) nodeEngineImpl.getEventService();
        EventServiceSegment segment = eventServiceImpl.getSegment(MapService.SERVICE_NAME, false);
        ConcurrentMap registrationIdMap = segment.getRegistrationIdMap();

        assertTrue(registrationIdMap.toString(), registrationIdMap.isEmpty());
    }

    private static void assertServerSideEventServiceCleared(HecatonInstance node) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(node);
        EventServiceImpl eventServiceImpl = (EventServiceImpl) nodeEngineImpl.getEventService();
        EventServiceSegment segment = eventServiceImpl.getSegment(MapService.SERVICE_NAME, false);
        ConcurrentMap registrationIdMap = segment.getRegistrationIdMap();

        assertTrue(registrationIdMap.toString(), registrationIdMap.isEmpty());
    }

    private static void assertNoUserListenerLeft(String mapName, HecatonInstance client) {
        ProxyManager proxyManager = ((HecatonClientProxy) client).client.getProxyManager();
        ClientContext context = proxyManager.getContext();
        ClientQueryCacheContext queryCacheContext = context.getQueryCacheContext();
        SubscriberContext subscriberContext = queryCacheContext.getSubscriberContext();
        QueryCacheEventService eventService = subscriberContext.getEventService();

        assertFalse(hasAnyListenerRegistered(eventService, mapName));
    }

    private static boolean hasAnyListenerRegistered(QueryCacheEventService eventService, String mapName) {
        ConcurrentMap<String, QueryCacheToListenerMapper> registrations
                = ((ClientQueryCacheEventService) eventService).getRegistrations();
        QueryCacheToListenerMapper queryCacheToListenerMapper = registrations.get(mapName);
        return queryCacheToListenerMapper != null && queryCacheToListenerMapper.hasAnyQueryCacheRegistered();
    }
}
