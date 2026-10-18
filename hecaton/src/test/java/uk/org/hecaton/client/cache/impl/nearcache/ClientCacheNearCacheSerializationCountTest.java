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
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.NearCacheConfig.LocalUpdatePolicy;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.adapter.DataStructureAdapter.DataStructureMethods;
import uk.org.hecaton.internal.adapter.DataStructureAdapterMethod;
import uk.org.hecaton.internal.adapter.ICacheDataStructureAdapter;
import uk.org.hecaton.internal.nearcache.NearCache;
import uk.org.hecaton.internal.nearcache.NearCacheManager;
import uk.org.hecaton.internal.nearcache.impl.AbstractNearCacheSerializationCountTest;
import uk.org.hecaton.internal.nearcache.impl.NearCacheSerializationCountConfigBuilder;
import uk.org.hecaton.internal.nearcache.impl.NearCacheTestContext;
import uk.org.hecaton.internal.nearcache.impl.NearCacheTestContextBuilder;
import uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.spi.CachingProvider;
import java.util.Collection;

import static uk.org.hecaton.cache.CacheTestSupport.createClientCachingProvider;
import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static uk.org.hecaton.config.EvictionPolicy.LRU;
import static uk.org.hecaton.config.InMemoryFormat.BINARY;
import static uk.org.hecaton.config.InMemoryFormat.NATIVE;
import static uk.org.hecaton.config.InMemoryFormat.OBJECT;
import static uk.org.hecaton.config.MaxSizePolicy.USED_NATIVE_MEMORY_PERCENTAGE;
import static uk.org.hecaton.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static uk.org.hecaton.config.NearCacheConfig.LocalUpdatePolicy.INVALIDATE;
import static uk.org.hecaton.internal.adapter.DataStructureAdapter.DataStructureMethods.GET;
import static uk.org.hecaton.internal.adapter.DataStructureAdapter.DataStructureMethods.GET_ALL;
import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.createNearCacheConfig;
import static uk.org.hecaton.spi.properties.ClusterProperty.CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static uk.org.hecaton.spi.properties.ClusterProperty.CACHE_INVALIDATION_MESSAGE_BATCH_SIZE;
import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_COUNT;
import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_OPERATION_THREAD_COUNT;
import static java.util.Arrays.asList;

/**
 * Near Cache serialization count tests for {@link ICache} on Hecaton clients.
 */
@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheNearCacheSerializationCountTest
        extends AbstractNearCacheSerializationCountTest<Data, String> {

    @Parameter
    public DataStructureMethods method;

    @Parameter(value = 1)
    public int[] keySerializationCounts;

    @Parameter(value = 2)
    public int[] keyDeserializationCounts;

    @Parameter(value = 3)
    public int[] valueSerializationCounts;

    @Parameter(value = 4)
    public int[] valueDeserializationCounts;

    @Parameter(value = 5)
    public InMemoryFormat cacheInMemoryFormat;

    @Parameter(value = 6)
    public InMemoryFormat nearCacheInMemoryFormat;

    @Parameter(value = 7)
    public Boolean invalidateOnChange;

    @Parameter(value = 8)
    public Boolean serializeKeys;

    @Parameter(value = 9)
    public LocalUpdatePolicy localUpdatePolicy;

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @Parameters(name = "method:{0} cacheFormat:{5} nearCacheFormat:{6} invalidateOnChange:{7} serializeKeys:{8} localUpdatePolicy:{9}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, null, null, null, null},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, true, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, true, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, true, false, INVALIDATE},
                {GET, newInt(1, 0, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, true, false, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, false, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, false, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, false, false, INVALIDATE},
                {GET, newInt(1, 0, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, false, false, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), BINARY, OBJECT, true, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 0, 0), BINARY, OBJECT, true, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), BINARY, OBJECT, true, false, INVALIDATE},
                {GET, newInt(1, 0, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 0, 0), BINARY, OBJECT, true, false, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), BINARY, OBJECT, false, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 0, 0), BINARY, OBJECT, false, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 0), BINARY, OBJECT, false, false, INVALIDATE},
                {GET, newInt(1, 0, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 0, 0), BINARY, OBJECT, false, false, CACHE_ON_UPDATE},

                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 1), newInt(1, 1, 1), OBJECT, null, null, null, null},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 1), OBJECT, BINARY, true, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(1, 1, 1), OBJECT, BINARY, true, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 1), OBJECT, BINARY, true, false, INVALIDATE},
                {GET, newInt(1, 0, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(1, 1, 1), OBJECT, BINARY, true, false, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 1), OBJECT, BINARY, false, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(1, 1, 1), OBJECT, BINARY, false, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 1), OBJECT, BINARY, false, false, INVALIDATE},
                {GET, newInt(1, 0, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(1, 1, 1), OBJECT, BINARY, false, false, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 0), OBJECT, OBJECT, true, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(1, 0, 0), OBJECT, OBJECT, true, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 0), OBJECT, OBJECT, true, false, INVALIDATE},
                {GET, newInt(1, 0, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(1, 0, 0), OBJECT, OBJECT, true, false, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 0), OBJECT, OBJECT, false, true, INVALIDATE},
                {GET, newInt(1, 1, 1), newInt(0, 0, 0), newInt(1, 0, 0), newInt(1, 0, 0), OBJECT, OBJECT, false, true, CACHE_ON_UPDATE},
                {GET, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 0), OBJECT, OBJECT, false, false, INVALIDATE},
                {GET, newInt(1, 0, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(1, 0, 0), OBJECT, OBJECT, false, false, CACHE_ON_UPDATE},

                {GET_ALL, newInt(1, 1, 1), newInt(0, 1, 1), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, null, null, null, null},
                {GET_ALL, newInt(1, 1, 1), newInt(0, 1, 1), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, true, true, INVALIDATE},
                {GET_ALL, newInt(1, 1, 1), newInt(0, 1, 1), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, true, true, CACHE_ON_UPDATE},
                {GET_ALL, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, true, false, INVALIDATE},
                {GET_ALL, newInt(1, 0, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(0, 1, 1), BINARY, BINARY, true, false, CACHE_ON_UPDATE},

                {GET_ALL, newInt(1, 1, 1), newInt(0, 1, 1), newInt(1, 1, 1), newInt(1, 1, 1), OBJECT, null, null, null, null},
                {GET_ALL, newInt(1, 1, 1), newInt(0, 1, 1), newInt(1, 1, 0), newInt(1, 1, 0), OBJECT, OBJECT, false, true, INVALIDATE},
                {GET_ALL, newInt(1, 1, 1), newInt(0, 1, 1), newInt(1, 0, 0), newInt(1, 0, 0), OBJECT, OBJECT, false, true, CACHE_ON_UPDATE},
                {GET_ALL, newInt(1, 1, 0), newInt(0, 0, 0), newInt(1, 1, 0), newInt(1, 1, 0), OBJECT, OBJECT, false, false, INVALIDATE},
                {GET_ALL, newInt(1, 0, 0), newInt(0, 0, 0), newInt(1, 0, 0), newInt(1, 0, 0), OBJECT, OBJECT, false, false, CACHE_ON_UPDATE},
        });
    }

    @Before
    public void setUp() {
        testMethod = method;
        expectedKeySerializationCounts = keySerializationCounts;
        expectedKeyDeserializationCounts = keyDeserializationCounts;
        expectedValueSerializationCounts = valueSerializationCounts;
        expectedValueDeserializationCounts = valueDeserializationCounts;
        if (nearCacheInMemoryFormat != null) {
            nearCacheConfig = createNearCacheConfig(nearCacheInMemoryFormat, serializeKeys)
                    .setInvalidateOnChange(invalidateOnChange)
                    .setLocalUpdatePolicy(localUpdatePolicy);
        }
    }

    @After
    public void tearDown() {
        hecatonFactory.shutdownAll();
    }

    @Override
    protected void addConfiguration(NearCacheSerializationCountConfigBuilder configBuilder) {
        configBuilder.append(method);
        configBuilder.append(cacheInMemoryFormat);
        configBuilder.append(nearCacheInMemoryFormat);
        configBuilder.append(invalidateOnChange);
        configBuilder.append(serializeKeys);
        configBuilder.append(localUpdatePolicy);
    }

    @Override
    protected void assumeThatMethodIsAvailable(DataStructureAdapterMethod method) {
        NearCacheTestUtils.assumeThatMethodIsAvailable(ICacheDataStructureAdapter.class, method);
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createContext() {
        Config config = getConfig()
                // we don't want to have the invalidations from the initial population being sent during this test
                .setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), String.valueOf(Integer.MAX_VALUE))
                .setProperty(CACHE_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), String.valueOf(Integer.MAX_VALUE))
                .setProperty(PARTITION_COUNT.getName(), "1")
                .setProperty(PARTITION_OPERATION_THREAD_COUNT.getName(), "1");
        prepareSerializationConfig(config.getSerializationConfig());

        HecatonInstance member = hecatonFactory.newHecatonInstance(config);
        CachingProvider memberProvider = createServerCachingProvider(member);
        HecatonServerCacheManager memberCacheManager = (HecatonServerCacheManager) memberProvider.getCacheManager();

        CacheConfig<K, V> cacheConfig = createCacheConfig(cacheInMemoryFormat);
        ICache<K, V> memberCache = memberCacheManager.createCache(DEFAULT_NEAR_CACHE_NAME, cacheConfig);

        return createNearCacheContextBuilder(cacheConfig)
                .setDataInstance(member)
                .setDataAdapter(new ICacheDataStructureAdapter<>(memberCache))
                .setMemberCacheManager(memberCacheManager)
                .build();
    }

    @Override
    protected <K, V> NearCacheTestContext<K, V, Data, String> createNearCacheContext() {
        CacheConfig<K, V> cacheConfig = createCacheConfig(cacheInMemoryFormat);
        return createNearCacheContextBuilder(cacheConfig).build();
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    private <K, V> CacheConfig<K, V> createCacheConfig(InMemoryFormat inMemoryFormat) {
        CacheConfig<K, V> cacheConfig = new CacheConfig<K, V>()
                .setName(DEFAULT_NEAR_CACHE_NAME)
                .setInMemoryFormat(inMemoryFormat)
                .setBackupCount(0)
                .setAsyncBackupCount(0);

        if (inMemoryFormat == NATIVE) {
            cacheConfig.getEvictionConfig()
                    .setEvictionPolicy(LRU)
                    .setMaxSizePolicy(USED_NATIVE_MEMORY_PERCENTAGE)
                    .setSize(90);
        }

        return cacheConfig;
    }

    private <K, V> NearCacheTestContextBuilder<K, V, Data, String> createNearCacheContextBuilder(CacheConfig<K, V> cacheConfig) {
        ClientConfig clientConfig = getClientConfig();
        if (nearCacheConfig != null) {
            clientConfig.addNearCacheConfig(nearCacheConfig);
        }
        prepareSerializationConfig(clientConfig.getSerializationConfig());

        HecatonClientProxy client = (HecatonClientProxy) hecatonFactory.newHecatonClient(clientConfig);

        CachingProvider provider = createClientCachingProvider(client);
        HecatonClientCacheManager cacheManager = (HecatonClientCacheManager) provider.getCacheManager();
        String cacheNameWithPrefix = cacheManager.getCacheNameWithPrefix(DEFAULT_NEAR_CACHE_NAME);
        ICache<K, V> clientCache = cacheManager.createCache(DEFAULT_NEAR_CACHE_NAME, cacheConfig);

        NearCacheManager nearCacheManager = ((ClientCacheProxy) clientCache).getContext()
                .getNearCacheManager(clientCache.getServiceName());
        NearCache<Data, String> nearCache = nearCacheManager.getNearCache(cacheNameWithPrefix);

        return new NearCacheTestContextBuilder<K, V, Data, String>(nearCacheConfig, client.getSerializationService())
                .setNearCacheInstance(client)
                .setNearCacheAdapter(new ICacheDataStructureAdapter<>(clientCache))
                .setNearCache(nearCache)
                .setNearCacheManager(nearCacheManager)
                .setCacheManager(cacheManager);
    }
}
