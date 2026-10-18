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

package uk.org.hecaton.client.cache;

import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.cache.impl.HecatonServerCachingProvider;
import uk.org.hecaton.cache.impl.event.CachePartitionLostEvent;
import uk.org.hecaton.cache.impl.event.CachePartitionLostListener;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.PartitionLostEventImpl;
import uk.org.hecaton.spi.impl.eventservice.EventRegistration;
import uk.org.hecaton.spi.impl.eventservice.EventService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import static uk.org.hecaton.cache.CacheTestSupport.createClientCachingProvider;
import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static uk.org.hecaton.cache.impl.ICacheService.SERVICE_NAME;
import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCachePartitionLostListenerTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void test_cachePartitionLostListener_registered() {

        final String cacheName = randomName();

        HecatonInstance instance = hecatonFactory.newHecatonInstance();
        final HecatonInstance client = hecatonFactory.newHecatonClient();

        final CachingProvider cachingProvider = createClientCachingProvider(client);
        final CacheManager cacheManager = cachingProvider.getCacheManager();
        final CacheConfig<Integer, String> cacheConfig = new CacheConfig<>();
        final Cache<Integer, String> cache = cacheManager.createCache(cacheName, cacheConfig);
        final ICache iCache = cache.unwrap(ICache.class);

        iCache.addPartitionLostListener(event -> {
        });

        assertRegistrationsSizeEventually(instance, cacheName, 1);
    }

    @Test
    public void test_cachePartitionLostListener_invoked() {
        final String cacheName = randomName();
        HecatonInstance instance = hecatonFactory.newHecatonInstance();
        final HecatonInstance client = hecatonFactory.newHecatonClient();

        final HecatonServerCachingProvider cachingProvider = createServerCachingProvider(instance);
        final CacheManager cacheManager = cachingProvider.getCacheManager();
        final CacheConfig<Integer, String> config = new CacheConfig<>();
        config.setBackupCount(0);
        cacheManager.createCache(cacheName, config);

        final CachingProvider clientCachingProvider = createClientCachingProvider(client);
        final CacheManager clientCacheManager = clientCachingProvider.getCacheManager();
        final Cache<Integer, String> cache = clientCacheManager.getCache(cacheName);

        final ICache iCache = cache.unwrap(ICache.class);

        final EventCollectingCachePartitionLostListener listener = new EventCollectingCachePartitionLostListener();
        iCache.addPartitionLostListener(listener);

        final CacheService cacheService = getNode(instance).getNodeEngine().getService(CacheService.SERVICE_NAME);
        final int partitionId = 5;
        cacheService.onPartitionLost(new PartitionLostEventImpl(partitionId, 0, null));

        assertCachePartitionLostEventEventually(listener, partitionId);
    }

    @Test
    public void test_cachePartitionLostListener_invoked_fromOtherNode() {

        final String cacheName = randomName();
        HecatonInstance instance1 = hecatonFactory.newHecatonInstance();
        HecatonInstance instance2 = hecatonFactory.newHecatonInstance();
        final HecatonInstance client = hecatonFactory.newHecatonClient();

        final HecatonServerCachingProvider cachingProvider = createServerCachingProvider(instance1);
        final CacheManager cacheManager = cachingProvider.getCacheManager();
        final CacheConfig<Integer, String> config = new CacheConfig<>();
        config.setBackupCount(0);
        cacheManager.createCache(cacheName, config);

        final CachingProvider clientCachingProvider = createClientCachingProvider(client);
        final CacheManager clientCacheManager = clientCachingProvider.getCacheManager();
        final Cache<Integer, String> cache = clientCacheManager.getCache(cacheName);

        final ICache iCache = cache.unwrap(ICache.class);

        final EventCollectingCachePartitionLostListener listener = new EventCollectingCachePartitionLostListener();
        iCache.addPartitionLostListener(listener);

        assertRegistrationsSizeEventually(instance1, cacheName, 1);
        assertRegistrationsSizeEventually(instance2, cacheName, 1);

        final CacheService cacheService1 = getNode(instance1).getNodeEngine().getService(CacheService.SERVICE_NAME);
        final CacheService cacheService2 = getNode(instance2).getNodeEngine().getService(CacheService.SERVICE_NAME);
        final int partitionId = 5;
        cacheService1.onPartitionLost(new PartitionLostEventImpl(partitionId, 0, null));
        cacheService2.onPartitionLost(new PartitionLostEventImpl(partitionId, 0, null));

        assertCachePartitionLostEventEventually(listener, partitionId);
    }

    @Test
    public void test_cachePartitionLostListener_removed() {
        final String cacheName = randomName();
        HecatonInstance instance = hecatonFactory.newHecatonInstance();
        final HecatonInstance client = hecatonFactory.newHecatonClient();

        final HecatonServerCachingProvider cachingProvider = createServerCachingProvider(instance);
        final CacheManager cacheManager = cachingProvider.getCacheManager();
        final CacheConfig<Integer, String> config = new CacheConfig<>();
        config.setBackupCount(0);
        cacheManager.createCache(cacheName, config);

        final CachingProvider clientCachingProvider = createClientCachingProvider(client);
        final CacheManager clientCacheManager = clientCachingProvider.getCacheManager();
        final Cache<Integer, String> cache = clientCacheManager.getCache(cacheName);
        final ICache iCache = cache.unwrap(ICache.class);

        final UUID registrationId = iCache.addPartitionLostListener(mock(CachePartitionLostListener.class));

        assertRegistrationsSizeEventually(instance, cacheName, 1);

        assertTrue(iCache.removePartitionLostListener(registrationId));
        assertRegistrationsSizeEventually(instance, cacheName, 0);
    }

    private void assertCachePartitionLostEventEventually(final EventCollectingCachePartitionLostListener listener,
                                                         final int partitionId) {
        assertTrueEventually(() -> {

            final List<CachePartitionLostEvent> events = listener.getEvents();
            assertFalse(events.isEmpty());
            assertEquals(partitionId, events.get(0).getPartitionId());

        });
    }

    private void assertRegistrationsSizeEventually(final HecatonInstance instance, final String cacheName, final int size) {
        assertTrueEventually(() -> {
            final EventService eventService = getNode(instance).getNodeEngine().getEventService();
            final Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, cacheName);
            assertEquals(size, registrations.size());
        });
    }

    private static class EventCollectingCachePartitionLostListener
            implements CachePartitionLostListener {

        private final List<CachePartitionLostEvent> events
                = Collections.synchronizedList(new LinkedList<>());

        EventCollectingCachePartitionLostListener() {
        }

        @Override
        public void partitionLost(CachePartitionLostEvent event) {
            this.events.add(event);
        }

        public List<CachePartitionLostEvent> getEvents() {
            synchronized (events) {
                return new ArrayList<>(events);
            }
        }
    }
}
