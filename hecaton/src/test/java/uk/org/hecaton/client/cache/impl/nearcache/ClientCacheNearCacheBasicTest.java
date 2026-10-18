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

package uk.org.hecaton.client.cache.impl.nearcache;

import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.cache.impl.HecatonServerCacheManager;
import uk.org.hecaton.client.cache.impl.ClientCacheProxy;
import uk.org.hecaton.client.cache.impl.HecatonClientCacheManager;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.adapter.DataStructureAdapter;
import uk.org.hecaton.internal.adapter.DataStructureAdapterMethod;
import uk.org.hecaton.internal.adapter.ICacheCacheLoader;
import uk.org.hecaton.internal.adapter.ICacheDataStructureAdapter;
import uk.org.hecaton.internal.nearcache.NearCache;
import uk.org.hecaton.internal.nearcache.NearCacheManager;
import uk.org.hecaton.internal.nearcache.impl.AbstractNearCacheBasicTest;
import uk.org.hecaton.internal.nearcache.impl.NearCacheTestContext;
import uk.org.hecaton.internal.nearcache.impl.NearCacheTestContextBuilder;
import uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.configuration.FactoryBuilder;
import javax.cache.spi.CachingProvider;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.cache.CacheTestSupport.createClientCachingProvider;
import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static uk.org.hecaton.config.EvictionPolicy.LRU;
import static uk.org.hecaton.config.InMemoryFormat.NATIVE;
import static uk.org.hecaton.config.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static uk.org.hecaton.config.NearCacheConfig.DEFAULT_LOCAL_UPDATE_POLICY;
import static uk.org.hecaton.config.NearCacheConfig.DEFAULT_MEMORY_FORMAT;
import static uk.org.hecaton.config.NearCacheConfig.DEFAULT_SERIALIZE_KEYS;
import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.createNearCacheConfig;
import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.Assert.assertEquals;

/**
 * Basic Near Cache tests for {@link ICache} on Hecaton clients.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheNearCacheBasicTest extends AbstractNearCacheBasicTest<Data, String> {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @Before
    public void setUp() {
        nearCacheConfig = createNearCacheConfig(DEFAULT_MEMORY_FORMAT, DEFAULT_SERIALIZE_KEYS)
                .setLocalUpdatePolicy(DEFAULT_LOCAL_UPDATE_POLICY);
    }

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void putAsyncToCacheAndThenGetFromClientNearCacheImmediately() {
        assumeThat(nearCacheConfig.getLocalUpdatePolicy())
                .as("Tests behaviour specific to CACHE_ON_UPDATE policy")
                .isEqualTo(NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE);
        // putAsync future is completed -> near cache contains the new value only with CACHE_ON_UPDATE policy
        NearCacheTestContext context = createContext(false);

        for (int i = 0; i < 10 * DEFAULT_RECORD_COUNT; i++) {
            Object key = context.nearCacheConfig.isSerializeKeys()
                    ? context.serializationService.toData(i) : i;
            String expectedValue = "value-" + i;
            CompletableFuture f = context.nearCacheAdapter.putAsync(i, expectedValue).toCompletableFuture();
            f.join();
            assertEquals(expectedValue, context.nearCache.get(key));
        }
    }

    @Override
    protected void assumeThatMethodIsAvailable(DataStructureAdapterMethod method) {
        NearCacheTestUtils.assumeThatMethodIsAvailable(ICacheDataStructureAdapter.class, method);
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext(boolean loaderEnabled) {
        Config config = getConfig();
        CacheConfig<K, V> cacheConfig = getCacheConfig(nearCacheConfig, loaderEnabled);

        HecatonInstance member = hecatonFactory.newHecatonInstance(config);
        CachingProvider memberProvider = createServerCachingProvider(member);
        HecatonServerCacheManager memberCacheManager = (HecatonServerCacheManager) memberProvider.getCacheManager();
        ICache<K, V> memberCache = memberCacheManager.createCache(DEFAULT_NEAR_CACHE_NAME, cacheConfig);
        ICacheDataStructureAdapter<K, V> dataAdapter = new ICacheDataStructureAdapter<>(memberCache);

        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder(cacheConfig);
        return builder
                .setDataInstance(member)
                .setDataAdapter(dataAdapter)
                .setMemberCacheManager(memberCacheManager)
                .build();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createNearCacheContext() {
        CacheConfig<K, V> cacheConfig = getCacheConfig(nearCacheConfig, false);
        NearCacheTestContextBuilder<K, V, Data, String> builder = createNearCacheContextBuilder(cacheConfig);
        return builder.build();
    }

    @Override
    protected Config getConfig() {
        return getBaseConfig();
    }

    protected ClientConfig getClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(NearCache.PROP_EXPIRATION_TASK_INITIAL_DELAY_SECONDS, "0");
        clientConfig.setProperty(NearCache.PROP_EXPIRATION_TASK_PERIOD_SECONDS, "1");
        return clientConfig.addNearCacheConfig(nearCacheConfig);
    }

    @SuppressWarnings("unchecked")
    private <K, V> CacheConfig<K, V> getCacheConfig(NearCacheConfig nearCacheConfig, boolean loaderEnabled) {
        CacheConfig<K, V> cacheConfig = new CacheConfig<K, V>()
                .setName(DEFAULT_NEAR_CACHE_NAME)
                .setInMemoryFormat(nearCacheConfig.getInMemoryFormat());

        if (nearCacheConfig.getInMemoryFormat() == NATIVE) {
            cacheConfig.getEvictionConfig()
                    .setEvictionPolicy(LRU)
                    .setMaxSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                    .setSize(90);
        }

        if (loaderEnabled) {
            cacheConfig
                    .setReadThrough(true)
                    .setCacheLoaderFactory(new FactoryBuilder.ClassFactory(ICacheCacheLoader.class));
        }

        return cacheConfig;
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder(CacheConfig<K, V> cacheConfig) {
        ClientConfig clientConfig = getClientConfig();

        HecatonClientProxy client = (HecatonClientProxy) hecatonFactory.newHecatonClient(clientConfig);
        CachingProvider provider = createClientCachingProvider(client);
        HecatonClientCacheManager cacheManager = (HecatonClientCacheManager) provider.getCacheManager();
        ICache<K, V> clientCache = cacheManager.createCache(DEFAULT_NEAR_CACHE_NAME, cacheConfig);

        NearCacheManager nearCacheManager = ((ClientCacheProxy) clientCache).getContext()
                .getNearCacheManager(clientCache.getServiceName());
        String cacheNameWithPrefix = cacheManager.getCacheNameWithPrefix(DEFAULT_NEAR_CACHE_NAME);
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(cacheNameWithPrefix);

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, client.getSerializationService())
                .setNearCacheInstance(client)
                .setNearCacheAdapter(new ICacheDataStructureAdapter<>(clientCache))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager)
                .setCacheManager(cacheManager);
    }

    @Override
    @Test
    public void whenGetAndReplaceIsUsedWithCacheOnUpdate_thenNearCacheShouldBePopulated() {
        super.whenEntryIsAddedWithCacheOnUpdate_thenNearCacheShouldBePopulated(DataStructureAdapter.DataStructureMethods.GET_AND_REPLACE);
    }
}
