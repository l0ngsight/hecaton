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

package uk.org.hecaton.cache;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.spi.CachingProvider;
import java.io.Serializable;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheListenerTest extends HecatonTestSupport {

    protected HecatonInstance hecatonInstance;

    protected CachingProvider getCachingProvider() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        HecatonInstance hz1 = factory.newHecatonInstance();
        HecatonInstance hz2 = factory.newHecatonInstance();

        warmUpPartitions(hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        hecatonInstance = hz1;

        return createServerCachingProvider(hecatonInstance);
    }

    @Test
    public void testSyncListener() throws Exception {
        CachingProvider cachingProvider = getCachingProvider();
        CacheManager cacheManager = cachingProvider.getCacheManager();

        final AtomicInteger counter = new AtomicInteger();

        CompleteConfiguration<String, String> config = new MutableConfiguration<String, String>()
                .setTypes(String.class, String.class).addCacheEntryListenerConfiguration(
                        new MutableCacheEntryListenerConfiguration<>(
                                FactoryBuilder.factoryOf(new TestListener(counter)), null, true, true));

        final Cache<String, String> cache = cacheManager.createCache("test", config);

        final int threadCount = 10;
        final int shutdownWaitTimeInSeconds = threadCount;
        final int putCount = 1000;
        final AtomicInteger actualPutCount = new AtomicInteger();
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final AtomicBoolean shutdown = new AtomicBoolean();
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                Random rand = new Random();
                for (int i1 = 0; i1 < putCount && !shutdown.get(); i1++) {
                    String key = String.valueOf(rand.nextInt(putCount));
                    String value = UUID.randomUUID().toString();
                    cache.put(key, value);
                    actualPutCount.incrementAndGet();
                }
                latch.countDown();
            }).start();
        }

        if (!latch.await(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS)) {
            shutdown.set(true);
            if (!latch.await(shutdownWaitTimeInSeconds, TimeUnit.SECONDS)) {
                fail("Cache operations have not finished in "
                        + (ASSERT_TRUE_EVENTUALLY_TIMEOUT + shutdownWaitTimeInSeconds)
                        + " seconds when sync listener is present!");
            }
        }
        assertEquals(actualPutCount.get(), counter.get());
    }

    @Test(timeout = 30000)
    public void testPutIfAbsentWithSyncListener_whenEntryExists() {
        CachingProvider cachingProvider = getCachingProvider();
        CacheManager cacheManager = cachingProvider.getCacheManager();

        CompleteConfiguration<String, String> config = new MutableConfiguration<String, String>()
                .setTypes(String.class, String.class).addCacheEntryListenerConfiguration(
                        new MutableCacheEntryListenerConfiguration<>(
                                FactoryBuilder.factoryOf(new TestListener(new AtomicInteger())), null, true, true));

        Cache<String, String> cache = cacheManager.createCache(randomString(), config);
        String key = randomString();
        cache.put(key, randomString());
        // there should not be any hanging due to sync listener
        cache.putIfAbsent(key, randomString());
    }

    @Test(timeout = 30000)
    public void testReplaceWithSyncListener_whenEntryNotExists() {
        CachingProvider cachingProvider = getCachingProvider();
        CacheManager cacheManager = cachingProvider.getCacheManager();

        CompleteConfiguration<String, String> config = new MutableConfiguration<String, String>()
                .setTypes(String.class, String.class).addCacheEntryListenerConfiguration(
                        new MutableCacheEntryListenerConfiguration<>(
                                FactoryBuilder.factoryOf(new TestListener(new AtomicInteger())), null, true, true));

        Cache<String, String> cache = cacheManager.createCache(randomString(), config);
        // there should not be any hanging due to sync listener
        cache.replace(randomString(), randomString());
    }

    @Test(timeout = 30000)
    public void testReplaceIfSameWithSyncListener_whenEntryNotExists() {
        CachingProvider cachingProvider = getCachingProvider();
        CacheManager cacheManager = cachingProvider.getCacheManager();

        CompleteConfiguration<String, String> config = new MutableConfiguration<String, String>()
                .setTypes(String.class, String.class).addCacheEntryListenerConfiguration(
                        new MutableCacheEntryListenerConfiguration<>(
                                FactoryBuilder.factoryOf(new TestListener(new AtomicInteger())), null, true, true));

        Cache<String, String> cache = cacheManager.createCache(randomString(), config);
        // there should not be any hanging due to sync listener
        cache.replace(randomString(), randomString(), randomString());
    }

    @Test(timeout = 30000)
    public void testReplaceIfSameWithSyncListener_whenValueIsNotSame() {
        CachingProvider cachingProvider = getCachingProvider();
        CacheManager cacheManager = cachingProvider.getCacheManager();

        CompleteConfiguration<String, String> config = new MutableConfiguration<String, String>()
                .setTypes(String.class, String.class).addCacheEntryListenerConfiguration(
                        new MutableCacheEntryListenerConfiguration<>(
                                FactoryBuilder.factoryOf(new TestListener(new AtomicInteger())), null, true, true));

        Cache<String, String> cache = cacheManager.createCache(randomString(), config);
        String key = randomString();
        cache.put(key, randomString());
        // there should not be any hanging due to sync listener
        cache.replace(key, randomString(), randomString());
    }

    @Test(timeout = 30000)
    public void testRemoveWithSyncListener_whenEntryNotExists() {
        CachingProvider cachingProvider = getCachingProvider();
        CacheManager cacheManager = cachingProvider.getCacheManager();

        CompleteConfiguration<String, String> config = new MutableConfiguration<String, String>()
                .setTypes(String.class, String.class).addCacheEntryListenerConfiguration(
                        new MutableCacheEntryListenerConfiguration<>(
                                FactoryBuilder.factoryOf(new TestListener(new AtomicInteger())), null, true, true));

        Cache<String, String> cache = cacheManager.createCache(randomString(), config);
        // there should not be any hanging due to sync listener
        cache.remove(randomString());
    }

    @Test(timeout = 30000)
    public void testRemoveIfSameWithSyncListener_whenEntryNotExists() {
        CachingProvider cachingProvider = getCachingProvider();
        CacheManager cacheManager = cachingProvider.getCacheManager();

        CompleteConfiguration<String, String> config = new MutableConfiguration<String, String>()
                .setTypes(String.class, String.class).addCacheEntryListenerConfiguration(
                        new MutableCacheEntryListenerConfiguration<>(
                                FactoryBuilder.factoryOf(new TestListener(new AtomicInteger())), null, true, true));

        Cache<String, String> cache = cacheManager.createCache(randomString(), config);
        // there should not be any hanging due to sync listener
        cache.remove(randomString(), randomString());
    }

    @Test(timeout = 30000)
    public void testRemoveIfSameWithSyncListener_whenValueIsNotSame() {
        CachingProvider cachingProvider = getCachingProvider();
        CacheManager cacheManager = cachingProvider.getCacheManager();

        CompleteConfiguration<String, String> config = new MutableConfiguration<String, String>()
                .setTypes(String.class, String.class).addCacheEntryListenerConfiguration(
                        new MutableCacheEntryListenerConfiguration<>(
                                FactoryBuilder.factoryOf(new TestListener(new AtomicInteger())), null, true, true));

        Cache<String, String> cache = cacheManager.createCache(randomString(), config);
        String key = randomString();
        cache.put(key, randomString());
        // there should not be any hanging due to sync listener
        cache.remove(key, randomString());
    }

    @Test(timeout = 30000)
    public void testGetAndReplaceWithSyncListener_whenEntryNotExists() {
        CachingProvider cachingProvider = getCachingProvider();
        CacheManager cacheManager = cachingProvider.getCacheManager();

        CompleteConfiguration<String, String> config = new MutableConfiguration<String, String>()
                .setTypes(String.class, String.class).addCacheEntryListenerConfiguration(
                        new MutableCacheEntryListenerConfiguration<>(
                                FactoryBuilder.factoryOf(new TestListener(new AtomicInteger())), null, true, true));

        Cache<String, String> cache = cacheManager.createCache(randomString(), config);
        // there should not be any hanging due to sync listener
        cache.getAndReplace(randomString(), randomString());
    }

    @Test(timeout = 30000)
    public void testGetAndRemoveWithSyncListener_whenEntryNotExists() {
        CachingProvider cachingProvider = getCachingProvider();
        CacheManager cacheManager = cachingProvider.getCacheManager();

        CompleteConfiguration<String, String> config = new MutableConfiguration<String, String>()
                .setTypes(String.class, String.class).addCacheEntryListenerConfiguration(
                        new MutableCacheEntryListenerConfiguration<>(
                                FactoryBuilder.factoryOf(new TestListener(new AtomicInteger())), null, true, true));

        Cache<String, String> cache = cacheManager.createCache(randomString(), config);
        // there should not be any hanging due to sync listener
        cache.getAndRemove(randomString());
    }

    @Test
    public void testSyncListener_shouldNotHang_whenHecatonInstanceShutdown() {
        CachingProvider provider = getCachingProvider();
        testSyncListener_shouldNotHang_AfterAction(randomMapName(), provider, () -> hecatonInstance.shutdown());
    }

    @Test
    public void testSyncListener_shouldNotHang_whenCacheClosed() {
        final CachingProvider provider = getCachingProvider();
        final String cacheName = randomMapName();

        testSyncListener_shouldNotHang_AfterAction(cacheName, provider, () -> {
            Cache cache = provider.getCacheManager().getCache(cacheName);
            cache.close();
        });
    }

    @Test
    public void testSyncListener_shouldNotHang_whenCacheDestroyed() {
        final CachingProvider provider = getCachingProvider();
        final String cacheName = randomMapName();

        testSyncListener_shouldNotHang_AfterAction(cacheName, provider, () -> provider.getCacheManager().destroyCache(cacheName));
    }

    private void testSyncListener_shouldNotHang_AfterAction(String cacheName, CachingProvider provider, Runnable action) {
        CacheManager cacheManager = provider.getCacheManager();

        CompleteConfiguration<String, String> config = new MutableConfiguration<String, String>()
                .addCacheEntryListenerConfiguration(new MutableCacheEntryListenerConfiguration<>(
                        FactoryBuilder.factoryOf(new TestListener(new AtomicInteger())), null, true,
                        true));

        final Cache<String, String> cache = cacheManager.createCache(cacheName, config);

        int threads = 4;
        final CountDownLatch latch = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            new Thread(() -> {
                Random rand = new Random();
                while (true) {
                    try {
                        cache.put(String.valueOf(rand.nextInt(100)), "value");
                    } catch (Throwable e) {
                        break;
                    }
                }
                latch.countDown();
            }).start();
        }

        // wait a little for putting threads to start
        sleepSeconds(3);

        action.run();

        assertOpenEventually("Cache operations should not hang when sync listener is present!", latch);
    }

    public static class TestListener
            implements CacheEntryCreatedListener<String, String>,
            CacheEntryUpdatedListener<String, String>,
            Serializable {

        private final AtomicInteger counter;

        public TestListener(AtomicInteger counter) {
            this.counter = counter;
        }

        @Override
        public void onCreated(Iterable<CacheEntryEvent<? extends String, ? extends String>> cacheEntryEvents)
                throws CacheEntryListenerException {
            onEvent(cacheEntryEvents);
        }

        @Override
        public void onUpdated(Iterable<CacheEntryEvent<? extends String, ? extends String>> cacheEntryEvents)
                throws CacheEntryListenerException {
            onEvent(cacheEntryEvents);
        }

        private void onEvent(Iterable<CacheEntryEvent<? extends String, ? extends String>> cacheEntryEvents) {
            int count = 0;
            for (CacheEntryEvent cacheEntryEvent : cacheEntryEvents) {
                count++;
            }
            // add some random delay to simulate sync listener
            LockSupport.parkNanos((long) (Math.random() * 10 * count));
            counter.addAndGet(count);
        }
    }

    @Test
    public void cacheEntryListenerCountIncreasedAndDecreasedCorrectly() {
        final CachingProvider provider = getCachingProvider();
        CacheManager cacheManager = provider.getCacheManager();

        CompleteConfiguration<String, String> config = new MutableConfiguration<String, String>()
                .addCacheEntryListenerConfiguration(new MutableCacheEntryListenerConfiguration<>(
                        FactoryBuilder.factoryOf(new TestListener(new AtomicInteger())), null, true,
                        true));

        cacheManager.createCache("MyCache", config);
    }
}
