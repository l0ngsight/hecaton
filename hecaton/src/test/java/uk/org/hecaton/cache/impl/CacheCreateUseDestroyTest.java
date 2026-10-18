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

package uk.org.hecaton.cache.impl;

import uk.org.hecaton.cache.CacheUtil;
import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.cache.jsr.JsrTestUtil;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.CacheSimpleEntryListenerConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.NativeMemoryConfig;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.memory.Capacity;
import uk.org.hecaton.memory.MemoryUnit;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.spi.CachingProvider;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import static uk.org.hecaton.cache.HecatonCachingProvider.propertiesByInstanceItself;
import static uk.org.hecaton.config.EvictionPolicy.LFU;
import static uk.org.hecaton.config.InMemoryFormat.BINARY;
import static uk.org.hecaton.config.InMemoryFormat.NATIVE;
import static uk.org.hecaton.config.InMemoryFormat.OBJECT;
import static uk.org.hecaton.config.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static uk.org.hecaton.test.Accessors.getNode;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class CacheCreateUseDestroyTest extends HecatonTestSupport {

    private static final Capacity NATIVE_MEMORY_SIZE = new Capacity(32, MemoryUnit.MEGABYTES);

    @Parameters(name = "{0}")
    public static Collection<Object[]> parameters() {
        return asList(
                new Object[]{OBJECT},
                new Object[]{BINARY},
                new Object[]{NATIVE}
        );
    }

    @Parameter
    public InMemoryFormat inMemoryFormat;

    protected CacheManager defaultCacheManager;
    protected Cache<String, String> cache;
    protected ICacheService cacheService;

    @Before
    public void setup() {
        assumptions();
        JsrTestUtil.setup();

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(1);
        HecatonInstance member = factory.newHecatonInstance(getConfig());
        CachingProvider provider = Caching.getCachingProvider();
        defaultCacheManager = provider.getCacheManager(null, null, propertiesByInstanceItself(member));
        cacheService = getNode(member).getNodeEngine().getService(ICacheService.SERVICE_NAME);
        CacheEntryListenerFactory.listener = null;
    }

    @After
    public void tearDown() {
        JsrTestUtil.cleanup();
    }

    protected void assumptions() {
        assumeThat(inMemoryFormat).isNotEqualTo(NATIVE);
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();

        CacheSimpleEntryListenerConfig entryListenerConfig = new CacheSimpleEntryListenerConfig();
        entryListenerConfig.setCacheEntryListenerFactory("uk.org.hecaton.cache.impl.CacheCreateUseDestroyTest$CacheEntryListenerFactory");
        entryListenerConfig.setOldValueRequired(true);
        entryListenerConfig.setSynchronous(true);

        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig()
                .setName("cache*")
                .setInMemoryFormat(inMemoryFormat)
                .setStatisticsEnabled(true)
                .setManagementEnabled(true)
                .setCacheEntryListeners(Collections.singletonList(entryListenerConfig));

        if (inMemoryFormat == NATIVE) {
            EvictionConfig evictionConfig = new EvictionConfig().setSize(90)
                    .setMaxSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE).setEvictionPolicy(LFU);
            cacheSimpleConfig.setEvictionConfig(evictionConfig);

            NativeMemoryConfig memoryConfig = new NativeMemoryConfig()
                    .setEnabled(true)
                    .setCapacity(NATIVE_MEMORY_SIZE)
                    .setAllocatorType(NativeMemoryConfig.MemoryAllocatorType.STANDARD);
            config.setNativeMemoryConfig(memoryConfig);
        }

        config.addCacheConfig(cacheSimpleConfig);
        return config;
    }

    @Test
    public void testCache_whenDestroyedByCacheManager() {
        String cacheName = randomMapName("cache");
        cache = defaultCacheManager.getCache(cacheName);
        cache.put("key", "value");
        cache.get("key");
        assertCreatedCache(cacheName);

        defaultCacheManager.destroyCache(cacheName);
        assertDestroyedCache(cacheName);
    }

    @Test
    public void testCache_whenDestroyedByICache_destroy() {
        String cacheName = randomMapName("cache");
        cache = defaultCacheManager.getCache(cacheName);
        DistributedObject internalCacheProxy = cache.unwrap(DistributedObject.class);
        cache.put("key", "value");
        cache.get("key");
        assertCreatedCache(cacheName);

        internalCacheProxy.destroy();
        assertDestroyedCache(cacheName);
    }

    private void assertCreatedCache(String cacheName) {
        assertStatistics(1);
        assertListenerCount(1, cacheName);
        assertMXBeanRegistrationStatus(true, cacheName, false);
        assertMXBeanRegistrationStatus(true, cacheName, true);
    }

    private void assertDestroyedCache(String cacheName) {
        assertStatistics(0);
        assertListenerCount(0, cacheName);
        assertTrue("CacheEntryListener was not properly closed", CacheEntryListenerFactory.listener.closed);
        assertMXBeanRegistrationStatus(false, cacheName, false);
        assertMXBeanRegistrationStatus(false, cacheName, true);
    }

    private void assertListenerCount(int expected, String cacheName) {
        String fullyQualifiedCacheName = CacheUtil.getDistributedObjectName(cacheName);
        CacheContext cacheContext = cacheService.getOrCreateCacheContext(fullyQualifiedCacheName);
        assertEquals("Unexpected listener count", expected, cacheContext.getCacheEntryListenerCount());
    }

    private void assertStatistics(int expectedHits) {
        ICache iCache = cache.unwrap(ICache.class);
        assertEquals("Unexpected cache hits count", expectedHits, iCache.getLocalCacheStatistics().getCacheHits());
    }

    private void assertMXBeanRegistrationStatus(boolean expectedStatus, String cacheName, boolean forStats) {
        CacheConfig cacheConfig = cacheService.findCacheConfig(cacheName);
        String cacheManagerName = cacheConfig.getUriString();
        assertEquals(format("Unexpected MXBean registration status for cacheManager %s and cacheName %s", cacheManagerName,
                cacheName), expectedStatus, MXBeanUtil.isRegistered(cacheManagerName, cacheName, forStats));
    }

    public static class CacheEntryListener implements CacheEntryCreatedListener<String, String>, Serializable, Closeable {

        volatile boolean closed;

        @Override
        public void onCreated(Iterable<CacheEntryEvent<? extends String, ? extends String>> cacheEntryEvents)
                throws CacheEntryListenerException {
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }
    }

    public static class CacheEntryListenerFactory implements Factory<CacheEntryListener> {

        public static volatile CacheEntryListener listener;

        @Override
        public CacheEntryListener create() {
            listener = new CacheEntryListener();
            return listener;
        }
    }
}
