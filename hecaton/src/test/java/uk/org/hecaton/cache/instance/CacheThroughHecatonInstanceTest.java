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

package uk.org.hecaton.cache.instance;

import uk.org.hecaton.cache.CacheNotExistsException;
import uk.org.hecaton.cache.HecatonCacheManager;
import uk.org.hecaton.cache.HecatonCachingProvider;
import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.cache.impl.HecatonInstanceCacheManager;
import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.core.ICacheManager;
import uk.org.hecaton.instance.impl.HecatonInstanceImpl;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.Properties;

import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CacheThroughHecatonInstanceTest extends HecatonTestSupport {

    private static final String CACHE_NAME = "MyCache";

    protected Config createConfig() {
        return new Config();
    }

    protected CacheSimpleConfig createCacheSimpleConfig(String cacheName) {
        CacheSimpleConfig cacheSimpleConfig = new CacheSimpleConfig();
        cacheSimpleConfig.setName(cacheName);
        return cacheSimpleConfig;
    }

    protected CacheConfig createCacheConfig(String cacheName) {
        return new CacheConfig(cacheName);
    }

    protected CachingProvider createCachingProvider(HecatonInstance instance) {
        return createServerCachingProvider(instance);
    }

    protected HecatonInstance createInstance() {
        TestHecatonInstanceFactory instanceFactory = createHecatonInstanceFactory();
        return instanceFactory.newHecatonInstance();
    }

    protected HecatonInstance createInstance(Config config) {
        TestHecatonInstanceFactory instanceFactory = createHecatonInstanceFactory();
        return instanceFactory.newHecatonInstance(config);
    }

    protected void shutdownOwnerInstance(HecatonInstance instance) {
        instance.shutdown();
    }

    protected Class<? extends Exception> getInstanceNotActiveExceptionType() {
        return HecatonInstanceNotActiveException.class;
    }

    @Test
    public void getCache_whenThereIsNoCacheConfig_thenFail() {
        assertThrows(CacheNotExistsException.class, () -> whenThereIsNoCacheConfig_thenFail(true));
    }

    @Test
    public void getDistributedObject_whenThereIsNoCacheConfig_thenFail() {
        assertThrows(CacheNotExistsException.class, () -> whenThereIsNoCacheConfig_thenFail(false));
    }

    private void whenThereIsNoCacheConfig_thenFail(boolean getCache) {
        HecatonInstance instance = createInstance();
        retrieveCache(instance, getCache);
    }

    @Test
    public void getCache_whenJCacheLibIsNotAvailable_thenFail() {
        final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        try {
            ClassLoader classLoader = new NonJCacheAwareClassLoader();
            Config config = createConfig();
            config.setClassLoader(classLoader);
            Thread.currentThread().setContextClassLoader(classLoader);
            HecatonInstance instance = createInstance(config);

            assertThrows(IllegalStateException.class, () -> retrieveCache(instance, true));
        } finally {
            Thread.currentThread().setContextClassLoader(tccl);
        }
    }

    @Test
    public void whenJCacheLibIsNotAvailable_thenOtherServicesWorks() {
        final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        try {
            ClassLoader classLoader = new NonJCacheAwareClassLoader();
            Config config = createConfig();
            config.setClassLoader(classLoader);
            Thread.currentThread().setContextClassLoader(classLoader);
            HecatonInstance instance = createInstance(config);

            IMap<Integer, String> map = instance.getMap(randomName());
            map.put(1, "Value-1");
            assertEquals("Value-1", map.get(1));
        } finally {
            Thread.currentThread().setContextClassLoader(tccl);
        }
    }

    @Test
    public void getCache_whenThereIsCacheConfigAndDefinedInConfig_thenRetrieveCacheSucceeds() {
        whenThereIsCacheConfigAndDefinedInConfig_thenRetrieveCacheSucceeds(true);
    }

    @Test
    public void getDistributedObject_whenThereIsCacheConfigAndDefinedInConfig_thenRetrieveCacheSucceeds() {
        whenThereIsCacheConfigAndDefinedInConfig_thenRetrieveCacheSucceeds(false);
    }

    private void whenThereIsCacheConfigAndDefinedInConfig_thenRetrieveCacheSucceeds(boolean getCache) {
        Config config = createConfig();
        config.addCacheConfig(createCacheSimpleConfig(CACHE_NAME));

        HecatonInstance instance = createInstance(config);

        Cache cache = retrieveCache(instance, getCache);
        assertNotNull(cache);
    }

    @Test
    public void getCache_whenThereIsCacheConfigAndCreatedByCacheManager_thenReturnsSameCache() {
        whenThereIsCacheConfigAndCreatedByCacheManager_thenReturnsSameCache(true);
    }

    @Test
    public void getDistributedObject_whenThereIsCacheConfigAndCreatedByCacheManager_thenReturnsSameCache() {
        whenThereIsCacheConfigAndCreatedByCacheManager_thenReturnsSameCache(false);
    }

    private void whenThereIsCacheConfigAndCreatedByCacheManager_thenReturnsSameCache(boolean getCache) {
        HecatonInstance instance = createInstance();

        CachingProvider cachingProvider = createCachingProvider(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager();

        Cache cache1 = cacheManager.createCache(CACHE_NAME, createCacheConfig(CACHE_NAME));
        assertNotNull(cache1);

        Cache cache2 = retrieveCache(instance, getCache);
        assertNotNull(cache2);

        // verify that they are same cache instance
        assertSame(cache1, cache2);
    }

    @Test
    public void getCache_whenThereIsCacheConfigWithURIandCreatedByCacheManager_thenReturnsSameCache() throws Exception {
        whenThereIsCacheConfigWithURIandCreatedByCacheManager_thenReturnsSameCache(true);
    }

    @Test
    public void getDistributedObject_whenThereIsCacheConfigWithURIandCreatedByCacheManager_thenReturnsSameCache()
            throws Exception {
        whenThereIsCacheConfigWithURIandCreatedByCacheManager_thenReturnsSameCache(false);
    }

    private void whenThereIsCacheConfigWithURIandCreatedByCacheManager_thenReturnsSameCache(boolean getCache) throws Exception {
        HecatonInstance instance = createInstance();

        CachingProvider cachingProvider = createCachingProvider(instance);
        Properties properties = HecatonCachingProvider.propertiesByInstanceItself(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager(new URI("MY-URI"), null, properties);

        Cache cache1 = cacheManager.createCache(CACHE_NAME, createCacheConfig(CACHE_NAME));
        assertNotNull(cache1);

        Cache cache2 = retrieveCache(instance, "MY-URI/" + CACHE_NAME, getCache);
        assertNotNull(cache2);

        // verify that they are same cache instance
        assertSame(cache1, cache2);
    }

    @Test
    public void getCache_whenThereIsCacheConfigWithSameNameButDifferentFullNameAndCreatedByCacheManager_thenFail() {
        assertThrows(CacheNotExistsException.class,
                () -> whenThereIsCacheConfigWithSameNameButDifferentFullNameAndCreatedByCacheManager_thenFail(true));
    }

    @Test
    public void getDistributedObject_whenThereIsCacheConfigWithSameNameButDifferentFullNameAndCreatedByCacheManager_thenFail() {
        assertThrows(CacheNotExistsException.class,
                () -> whenThereIsCacheConfigWithSameNameButDifferentFullNameAndCreatedByCacheManager_thenFail(false));
    }

    private void whenThereIsCacheConfigWithSameNameButDifferentFullNameAndCreatedByCacheManager_thenFail(boolean getCache)
            throws Exception {
        HecatonInstance instance = createInstance();

        CachingProvider cachingProvider = createCachingProvider(instance);
        Properties properties = HecatonCachingProvider.propertiesByInstanceItself(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager(new URI("MY-URI"), null, properties);

        Cache cache1 = cacheManager.createCache(CACHE_NAME, createCacheConfig(CACHE_NAME));
        assertNotNull(cache1);

        retrieveCache(instance, getCache);
    }

    @Test
    public void getCache_whenThereIsCacheConfigAndCreatedByInstance_thenReturnSameCache() {
        whenThereIsCacheConfigAndCreatedByInstance_thenReturnSameCache(true);
    }

    @Test
    public void getDistributedObject_whenThereIsCacheConfigAndCreatedByInstance_thenReturnSameCache() {
        whenThereIsCacheConfigAndCreatedByInstance_thenReturnSameCache(false);
    }

    private void whenThereIsCacheConfigAndCreatedByInstance_thenReturnSameCache(boolean getCache) {
        Config config = createConfig();
        config.addCacheConfig(createCacheSimpleConfig(CACHE_NAME));

        HecatonInstance instance = createInstance(config);

        Cache cache1 = retrieveCache(instance, getCache);
        assertNotNull(cache1);

        CachingProvider cachingProvider = createCachingProvider(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager();

        Cache cache2 = cacheManager.getCache(CACHE_NAME);
        assertNotNull(cache2);

        // verify that they are same cache instance
        assertSame(cache1, cache2);
    }

    @Test
    public void getCache_whenThereIsCacheConfigWithSameNameButDifferentFullNameAndCreatedByInstance_thenReturnDifferentCache()
            throws Exception {
        whenThereIsCacheConfigWithSameNameButDifferentFullNameAndCreatedByInstance_thenReturnDifferentCache(true);
    }

    @Test
    public void getDistributedObject_whenThereIsCacheConfigWithSameNameButDifferentFullNameAndCreatedByInstance_thenReturnDifferentCache()
            throws Exception {
        whenThereIsCacheConfigWithSameNameButDifferentFullNameAndCreatedByInstance_thenReturnDifferentCache(false);
    }

    private void whenThereIsCacheConfigWithSameNameButDifferentFullNameAndCreatedByInstance_thenReturnDifferentCache(
            boolean getCache) throws Exception {
        Config config = createConfig();
        config.addCacheConfig(createCacheSimpleConfig(CACHE_NAME));

        HecatonInstance instance = createInstance(config);

        Cache cache1 = retrieveCache(instance, getCache);
        assertNotNull(cache1);

        CachingProvider cachingProvider = createCachingProvider(instance);
        Properties properties = HecatonCachingProvider.propertiesByInstanceItself(instance);
        CacheManager cacheManager = cachingProvider.getCacheManager(new URI("MY-URI"), null, properties);

        Cache cache2 = cacheManager.getCache(CACHE_NAME);
        assertNotNull(cache2);

        assertNotEquals(cache1, cache2);
    }

    @Test
    public void getCache_whenOwnerInstanceIsShutdown_thenOperateOnCacheFails() {
        whenOwnerInstanceIsShutdown_thenOperateOnCacheFails(true);
    }

    @Test
    public void getDistributedObject_whenOwnerInstanceIsShutdown_thenOperateOnCacheFails() {
        whenOwnerInstanceIsShutdown_thenOperateOnCacheFails(false);
    }

    private void whenOwnerInstanceIsShutdown_thenOperateOnCacheFails(boolean getCache) {
        Config config = createConfig();
        config.addCacheConfig(createCacheSimpleConfig(CACHE_NAME));

        HecatonInstance instance = createInstance(config);

        Cache<Integer, Integer> cache = retrieveCache(instance, getCache);
        assertNotNull(cache);

        cache.put(1, 1);

        shutdownOwnerInstance(instance);

        try {
            cache.put(2, 2);
            fail("Operation on cache must fails because owner instance is not active!");
        } catch (Throwable t) {
            Class<? extends Exception> expectedExceptionType = getInstanceNotActiveExceptionType();
            Class<? extends Throwable> actualExceptionType = t.getClass();
            assertTrue(
                    "Expected exception type: " + expectedExceptionType + ", but actual exception type: " + actualExceptionType,
                    expectedExceptionType.isAssignableFrom(actualExceptionType));
        }
    }

    @Test
    public void getCache_whenCacheIsDestroyed_thenCacheIsRemovedFromDistributedObject() {
        getCache_whenCacheIsDestroyed_thenCacheIsRemovedFromDistributedObject(true);
    }

    @Test
    public void getDistributedObject_getCache_whenCacheIsDestroyed_thenCacheIsRemovedFromDistributedObject() {
        getCache_whenCacheIsDestroyed_thenCacheIsRemovedFromDistributedObject(false);
    }

    private void getCache_whenCacheIsDestroyed_thenCacheIsRemovedFromDistributedObject(boolean getCache) {
        Config config = createConfig();
        config.addCacheConfig(createCacheSimpleConfig(CACHE_NAME));

        HecatonInstance instance = createInstance(config);

        ICache cache = retrieveCache(instance, getCache);
        assertNotNull(cache);

        assertContains(instance.getDistributedObjects(), cache);

        cache.destroy();
        assertNotContains(instance.getDistributedObjects(), cache);
    }

    @Test
    public void getCache_whenOtherHecatonExceptionIsThrown_thenFail() {
        // when one attempts to getCache but a HecatonException other than ServiceNotFoundException is thrown
        HecatonInstanceImpl hzInstanceImpl = mock(HecatonInstanceImpl.class);
        when(hzInstanceImpl.getDistributedObject(anyString(), anyString()))
                .thenThrow(new HecatonException("mock hz exception"));

        // then the thrown HecatonException is rethrown by getCache
        ICacheManager hzCacheManager = new HecatonInstanceCacheManager(hzInstanceImpl);
        assertThrows(HecatonException.class, () -> hzCacheManager.getCache("any-cache"));
    }

    @Test
    public void cacheConfigIsAvailableOnAllMembers_afterGetCacheCompletes() {
        Config config = createConfig();
        config.addCacheConfig(createCacheSimpleConfig(CACHE_NAME));

        TestHecatonInstanceFactory instanceFactory = createHecatonInstanceFactory();

        HecatonInstance instance1 = instanceFactory.newHecatonInstance(config);
        HecatonInstance instance2 = instanceFactory.newHecatonInstance(config);

        ICacheService cacheServiceOnInstance2 = getNodeEngineImpl(instance2).getService(ICacheService.SERVICE_NAME);
        retrieveCache(instance1, true);
        assertNotNull("Cache config was not available on other instance after cache proxy was created",
                cacheServiceOnInstance2.getCacheConfig(
                HecatonCacheManager.CACHE_MANAGER_PREFIX + CACHE_NAME));
    }

    private static ICache<Integer, Integer> retrieveCache(HecatonInstance instance, boolean getCache) {
        return retrieveCache(instance, CACHE_NAME, getCache);
    }

    @SuppressWarnings("unchecked")
    private static ICache<Integer, Integer> retrieveCache(HecatonInstance instance, String cacheName, boolean getCache) {
        return getCache
                ? instance.getCacheManager().getCache(cacheName)
                : (ICache) instance.getDistributedObject(ICacheService.SERVICE_NAME,
                HecatonCacheManager.CACHE_MANAGER_PREFIX + cacheName);
    }

    private static class NonJCacheAwareClassLoader extends ClassLoader {

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            if (name.startsWith("javax.cache.")) {
                throw new ClassNotFoundException("Couldn't load class " + name + ". Because JCache is disabled!");
            }
            return super.loadClass(name);
        }
    }
}
