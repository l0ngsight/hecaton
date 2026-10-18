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

package uk.org.hecaton.internal.diagnostics;

import uk.org.hecaton.cache.CacheEventType;
import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.cache.impl.CacheEventData;
import uk.org.hecaton.cache.impl.CacheEventDataImpl;
import uk.org.hecaton.cache.impl.CacheEventSet;
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.cache.impl.HecatonServerCacheManager;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.collection.IList;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.collection.ISet;
import uk.org.hecaton.collection.ItemEvent;
import uk.org.hecaton.collection.ItemListener;
import uk.org.hecaton.collection.impl.collection.CollectionEvent;
import uk.org.hecaton.collection.impl.list.ListService;
import uk.org.hecaton.collection.impl.queue.QueueEvent;
import uk.org.hecaton.collection.impl.queue.QueueService;
import uk.org.hecaton.collection.impl.set.SetService;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.ItemEventType;
import uk.org.hecaton.internal.longregister.LongRegisterService;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.util.ItemCounter;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.event.EntryEventData;
import uk.org.hecaton.map.listener.EntryAddedListener;
import uk.org.hecaton.map.listener.EntryRemovedListener;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.eventservice.impl.EventServiceImpl;
import uk.org.hecaton.spi.impl.eventservice.impl.LocalEventDispatcher;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static uk.org.hecaton.core.EntryEventType.ADDED;
import static uk.org.hecaton.core.EntryEventType.REMOVED;
import static uk.org.hecaton.core.EntryEventType.UPDATED;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EventQueuePluginTest extends AbstractDiagnosticsPluginTest {

    private static final int EVENT_COUNTER = 1000;

    private static final String MAP_NAME = "myMap";
    private static final String CACHE_NAME = "myCache";
    private static final String QUEUE_NAME = "myQueue";
    private static final String LIST_NAME = "myList";
    private static final String SET_NAME = "mySet";

    private final CountDownLatch listenerLatch = new CountDownLatch(1);

    private HecatonInstance hz;
    private EventQueuePlugin plugin;
    private ItemCounter<String> itemCounter;

    @Before
    public void setup() {
        Config config = new Config()
                .setProperty(EventQueuePlugin.PERIOD_SECONDS.getName(), "1")
                .setProperty(EventQueuePlugin.SAMPLES.getName(), "100")
                .setProperty(EventQueuePlugin.THRESHOLD.getName(), "10");

        hz = createHecatonInstance(config);

        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        EventServiceImpl eventService = (EventServiceImpl) nodeEngine.getEventService();

        plugin = new EventQueuePlugin(nodeEngine.getLogger(EventQueuePlugin.class), eventService.getEventExecutor(), nodeEngine.getProperties());
        plugin.onStart();

        itemCounter = plugin.getOccurrenceMap();

        warmUpPartitions(hz);
    }

    @Test
    public void testMap() {
        final IMap<Integer, Integer> map = hz.getMap(MAP_NAME);
        map.addLocalEntryListener((EntryAddedListener<Integer, Integer>) event -> assertOpenEventually(listenerLatch));
        map.addLocalEntryListener((EntryRemovedListener<Integer, Integer>) event -> assertOpenEventually(listenerLatch));

        spawn((Runnable) () -> {
            Random random = new Random();
            for (int i = 0; i < EVENT_COUNTER; i++) {
                int key = random.nextInt(Integer.MAX_VALUE);
                map.putAsync(key, 23);
                map.removeAsync(key);
            }
        });

        assertContainsEventually(
                "IMap '" + MAP_NAME + "' ADDED sampleCount=",
                "IMap '" + MAP_NAME + "' REMOVED sampleCount=");
    }

    @Test
    public void testCache() {
        CompleteConfiguration<Integer, Integer> cacheConfig = new MutableConfiguration<Integer, Integer>()
                .addCacheEntryListenerConfiguration(new MutableCacheEntryListenerConfiguration<>(
                        FactoryBuilder.factoryOf(new TestCacheListener()), null, true, true));

        CachingProvider memberProvider = createServerCachingProvider(hz);
        HecatonServerCacheManager memberCacheManager = (HecatonServerCacheManager) memberProvider.getCacheManager();
        final ICache<Integer, Integer> cache = memberCacheManager.createCache(CACHE_NAME, cacheConfig);

        spawn((Runnable) () -> {
            Random random = new Random();
            for (int i = 0; i < EVENT_COUNTER; i++) {
                int key = random.nextInt(Integer.MAX_VALUE);
                cache.putAsync(key, 23);
                cache.removeAsync(key);
            }
        });

        assertContainsEventually(
                "ICache '/hz/" + CACHE_NAME + "' CREATED sampleCount=",
                "ICache '/hz/" + CACHE_NAME + "' REMOVED sampleCount=");
    }

    @Test
    public void testQueue() {
        final IQueue<Integer> queue = hz.getQueue(QUEUE_NAME);
        queue.addItemListener(new TestItemListener(), true);

        spawn((Runnable) () -> {
            Random random = new Random();
            for (int i = 0; i < EVENT_COUNTER; i++) {
                int key = random.nextInt(Integer.MAX_VALUE);
                queue.add(key);
                queue.poll();
            }
        });

        assertContainsEventually(
                "IQueue '" + QUEUE_NAME + "' ADDED sampleCount=",
                "IQueue '" + QUEUE_NAME + "' REMOVED sampleCount=");
    }

    @Test
    public void testList() {
        final IList<Integer> list = hz.getList(LIST_NAME);
        list.addItemListener(new TestItemListener(), true);

        spawn((Runnable) () -> {
            Random random = new Random();
            for (int i = 0; i < EVENT_COUNTER; i++) {
                int key = random.nextInt(EVENT_COUNTER);
                list.add(key);
            }
        });

        assertContainsEventually("IList '" + LIST_NAME + "' ADDED sampleCount=");
    }

    @Test
    public void testSet() {
        final ISet<Integer> set = hz.getSet(SET_NAME);
        set.addItemListener(new TestItemListener(), true);

        spawn((Runnable) () -> {
            Random random = new Random();
            for (int i = 0; i < EVENT_COUNTER; i++) {
                int key = random.nextInt(Integer.MAX_VALUE);
                set.add(key);
                set.remove(key);
            }
        });

        assertContainsEventually(
                "ISet '" + SET_NAME + "' ADDED sampleCount=",
                "ISet '" + SET_NAME + "' REMOVED sampleCount=");
    }

    @Test
    public void testSampleRunnable() {
        Address caller = new Address();
        Data data = mock(Data.class);

        EntryEventData mapEventAdded = new EntryEventData("source", "mapName", caller, data, data, data, ADDED.getType());
        EntryEventData mapEventUpdated = new EntryEventData("source", "mapName", caller, data, data, data, UPDATED.getType());
        EntryEventData mapEventRemoved = new EntryEventData("source", "mapName", caller, data, data, data, REMOVED.getType());
        assertSampleRunnable("IMap 'mapName' ADDED", mapEventAdded, MapService.SERVICE_NAME);
        assertSampleRunnable("IMap 'mapName' UPDATED", mapEventUpdated, MapService.SERVICE_NAME);
        assertSampleRunnable("IMap 'mapName' REMOVED", mapEventRemoved, MapService.SERVICE_NAME);

        CacheEventData cacheEventCreated = new CacheEventDataImpl("cacheName", CacheEventType.CREATED, data, data, data, true);
        CacheEventData cacheEventUpdated = new CacheEventDataImpl("cacheName", CacheEventType.UPDATED, data, data, data, true);
        CacheEventData cacheEventRemoved = new CacheEventDataImpl("cacheName", CacheEventType.REMOVED, data, data, data, true);
        CacheEventSet CacheEventSetCreated = new CacheEventSet(CacheEventType.CREATED, singleton(cacheEventCreated), 1);
        CacheEventSet CacheEventSetUpdated = new CacheEventSet(CacheEventType.UPDATED, singleton(cacheEventUpdated), 1);
        CacheEventSet cacheEventSetRemoved = new CacheEventSet(CacheEventType.REMOVED, singleton(cacheEventRemoved), 1);
        assertSampleRunnable("ICache 'cacheName' CREATED", CacheEventSetCreated, CacheService.SERVICE_NAME);
        assertSampleRunnable("ICache 'cacheName' UPDATED", CacheEventSetUpdated, CacheService.SERVICE_NAME);
        assertSampleRunnable("ICache 'cacheName' REMOVED", cacheEventSetRemoved, CacheService.SERVICE_NAME);

        List<CacheEventData> cacheEventData = asList(cacheEventCreated, cacheEventUpdated, cacheEventRemoved);
        Set<CacheEventData> cacheEvents = new HashSet<>(cacheEventData);
        CacheEventSet cacheEventSetAll = new CacheEventSet(CacheEventType.EXPIRED, cacheEvents, 1);
        assertCacheEventSet(cacheEventSetAll,
                "ICache 'cacheName' CREATED",
                "ICache 'cacheName' UPDATED",
                "ICache 'cacheName' REMOVED");

        QueueEvent queueEventAdded = new QueueEvent("queueName", data, ItemEventType.ADDED, caller);
        QueueEvent queueEventRemoved = new QueueEvent("queueName", data, ItemEventType.REMOVED, caller);
        assertSampleRunnable("IQueue 'queueName' ADDED", queueEventAdded, QueueService.SERVICE_NAME);
        assertSampleRunnable("IQueue 'queueName' REMOVED", queueEventRemoved, QueueService.SERVICE_NAME);

        CollectionEvent setEventAdded = new CollectionEvent("setName", data, ItemEventType.ADDED, caller);
        CollectionEvent setEventRemoved = new CollectionEvent("setName", data, ItemEventType.REMOVED, caller);
        assertSampleRunnable("ISet 'setName' ADDED", setEventAdded, SetService.SERVICE_NAME);
        assertSampleRunnable("ISet 'setName' REMOVED", setEventRemoved, SetService.SERVICE_NAME);

        CollectionEvent listEventAdded = new CollectionEvent("listName", data, ItemEventType.ADDED, caller);
        CollectionEvent listEventRemoved = new CollectionEvent("listName", data, ItemEventType.REMOVED, caller);
        assertSampleRunnable("IList 'listName' ADDED", listEventAdded, ListService.SERVICE_NAME);
        assertSampleRunnable("IList 'listName' REMOVED", listEventRemoved, ListService.SERVICE_NAME);

        assertSampleRunnable("Object", new Object(), LongRegisterService.SERVICE_NAME);

        assertSampleRunnable(new TestEvent(), TestEvent.class.getName());
    }

    private void assertContainsEventually(final String... messages) {
        try {
            assertTrueEventually(() -> {
                plugin.run(logWriter);

                //System.out.println(getContent());

                for (String message : messages) {
                    assertContains(message);
                }
            });
        } finally {
            listenerLatch.countDown();
        }
    }

    private void assertCacheEventSet(CacheEventSet cacheEventSet, String... expectedKeys) {
        LocalEventDispatcher e = createLocalEventDispatcher(cacheEventSet, CacheService.SERVICE_NAME);
        assertSampleRunnable(e, expectedKeys);
    }

    private void assertSampleRunnable(String expectedKey, Object event, String serviceName) {
        assertSampleRunnable(createLocalEventDispatcher(event, serviceName), expectedKey);
    }

    private void assertSampleRunnable(Runnable runnable, String... expectedKeys) {
        assertEqualsStringFormat("Expected to sample %d keys, but got %d", expectedKeys.length, plugin.sampleRunnable(runnable));
        Set<String> actualKeys = itemCounter.keySet();
        for (String expectedKey : expectedKeys) {
            assertTrue("Expected to find key [" + expectedKey + "] in " + actualKeys, actualKeys.contains(expectedKey));
        }

        itemCounter.clear();
    }

    private LocalEventDispatcher createLocalEventDispatcher(Object event, String serviceName) {
        EventServiceImpl eventService = mock(EventServiceImpl.class);
        return new LocalEventDispatcher(eventService, serviceName, event, null, 1, 1);
    }

    private final class TestCacheListener
            implements CacheEntryCreatedListener<Integer, Integer>, CacheEntryRemovedListener<Integer, Integer>, Serializable {

        TestCacheListener() {
        }

        @Override
        public void onCreated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> cacheEntryEvents)
                throws CacheEntryListenerException {
            assertOpenEventually(listenerLatch);
        }

        @Override
        public void onRemoved(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> cacheEntryEvents)
                throws CacheEntryListenerException {
            assertOpenEventually(listenerLatch);
        }
    }

    private final class TestItemListener implements ItemListener<Integer> {

        @Override
        public void itemAdded(ItemEvent<Integer> item) {
            assertOpenEventually(listenerLatch);
        }

        @Override
        public void itemRemoved(ItemEvent<Integer> item) {
            assertOpenEventually(listenerLatch);
        }
    }

    private static class TestEvent implements Runnable {

        @Override
        public void run() {
        }
    }
}
