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

import uk.org.hecaton.cache.impl.HecatonServerCachingProvider;
import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.client.cache.impl.HecatonClientCachingProvider;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonInstanceImpl;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.TestUtil;
import uk.org.hecaton.spi.impl.proxyservice.ProxyService;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.spi.tenantcontrol.TenantControl;
import uk.org.hecaton.test.HecatonTestSupport;
import org.junit.After;
import org.junit.Before;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;

import static uk.org.hecaton.cache.impl.maxsize.impl.EntryCountCacheEvictionChecker.calculateMaxPartitionSize;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static java.lang.Integer.parseInt;
import static org.junit.Assert.assertEquals;

public abstract class CacheTestSupport extends HecatonTestSupport {

    protected CachingProvider cachingProvider;
    protected CacheManager cacheManager;

    protected abstract HecatonInstance getHecatonInstance();

    @Before
    public void setup() {
        onSetup();
        cachingProvider = getCachingProvider();
        cacheManager = cachingProvider.getCacheManager();
    }

    @After
    public final void tearDown() {
        if (cacheManager != null && !cacheManager.isClosed()) {
            Iterable<String> cacheNames = cacheManager.getCacheNames();
            for (String name : cacheNames) {
                cacheManager.destroyCache(name);
            }
            cacheManager.close();
        }
        if (cachingProvider != null) {
            cachingProvider.close();
        }
        onTearDown();
    }

    protected abstract void onSetup();

    protected abstract void onTearDown();

    protected <K, V> ICache<K, V> createCache() {
        String cacheName = randomString();
        Cache<K, V> cache = cacheManager.<K, V, Configuration>createCache(cacheName, createCacheConfig());
        return (ICache<K, V>) cache;
    }

    protected <K, V> ICache<K, V> createCache(String cacheName) {
        Cache<K, V> cache = cacheManager.<K, V, Configuration>createCache(cacheName, createCacheConfig());
        return (ICache<K, V>) cache;
    }

    protected <K, V> ICache<K, V> createCache(CacheConfig<K, V> config) {
        String cacheName = randomString();
        Cache<K, V> cache = cacheManager.<K, V, Configuration>createCache(cacheName, config);
        return (ICache<K, V>) cache;
    }

    protected <K, V> ICache<K, V> createCache(String cacheName, CacheConfig<K, V> config) {
        Cache<K, V> cache = cacheManager.<K, V, Configuration>createCache(cacheName, config);
        return (ICache<K, V>) cache;
    }

    protected Config createConfig() {
        return new Config();
    }

    protected <K, V> CacheConfig<K, V> createCacheConfig() {
        CacheConfig<K, V> cacheConfig = new CacheConfig<>();
        cacheConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        cacheConfig.setStatisticsEnabled(true);
        return cacheConfig;
    }

    protected <K, V> CacheConfig<K, V> getCacheConfigWithMaxSize(int maxCacheSize) {
        CacheConfig<K, V> config = createCacheConfig();
        config.getEvictionConfig().setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT);
        config.getEvictionConfig().setSize(maxCacheSize);
        return config;
    }

    protected CachingProvider getCachingProvider() {
        return getCachingProvider(getHecatonInstance());
    }

    protected CachingProvider getCachingProvider(HecatonInstance instance) {
        HecatonInstanceImpl hecatonInstanceImpl = TestUtil.getHecatonInstanceImpl(instance);
        return createServerCachingProvider(hecatonInstanceImpl);
    }

    protected int getMaxCacheSizeWithoutEviction(CacheConfig cacheConfig) {
        int maxEntryCount = cacheConfig.getEvictionConfig().getSize();
        return calculateMaxPartitionSize(maxEntryCount, getPartitionCount());
    }

    protected int getMaxCacheSizeWithEviction(CacheConfig cacheConfig) {
        int maxEntryCount = cacheConfig.getEvictionConfig().getSize();
        int partitionCount = getPartitionCount();
        return 10 * partitionCount * calculateMaxPartitionSize(maxEntryCount, partitionCount);
    }

    private int getPartitionCount() {
        try {
            Node node = getNode(getHecatonInstance());
            return node.getProperties().getInteger(ClusterProperty.PARTITION_COUNT);
        } catch (IllegalArgumentException e) {
            return parseInt(ClusterProperty.PARTITION_COUNT.getDefaultValue());
        }
    }

    protected void assertThatNoCacheEvictionHappened(ICache cache) {
        try {
            assertEquals("there should be no evicted values", 0, cache.getLocalCacheStatistics().getCacheEvictions());
        } catch (UnsupportedOperationException e) {
            // cache statistics are not supported on clients yet
            ignore(e);
        }
    }

    public static ICacheService getCacheService(HecatonInstance instance) {
        return getNodeEngineImpl(instance).getService(ICacheService.SERVICE_NAME);
    }

    public static ProxyService getProxyService(HecatonInstance instance) {
        return getNodeEngineImpl(instance).getProxyService();
    }

    public static TenantControl getTenantControl(HecatonInstance hz, ICache<?, ?> cache) {
        return getNodeEngineImpl(hz)
                .getTenantControlService()
                .getTenantControl(ICacheService.SERVICE_NAME, CacheUtil.getDistributedObjectName(cache.getName()));
    }

    public static TenantControl getTenantControl(HecatonInstance hz, CacheConfig cacheConfig) {
        return getNodeEngineImpl(hz)
                .getTenantControlService()
                .getTenantControl(ICacheService.SERVICE_NAME, CacheUtil.getDistributedObjectName(cacheConfig.getName()));
    }

    public static HecatonServerCachingProvider createServerCachingProvider(HecatonInstance instance) {
        return new HecatonServerCachingProvider(instance);
    }

    public static HecatonClientCachingProvider createClientCachingProvider(HecatonInstance instance) {
        return new HecatonClientCachingProvider(instance);
    }
}
