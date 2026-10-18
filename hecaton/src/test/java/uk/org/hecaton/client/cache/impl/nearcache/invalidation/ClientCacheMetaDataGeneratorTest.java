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

package uk.org.hecaton.client.cache.impl.nearcache.invalidation;

import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.cache.impl.CacheEventHandler;
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.client.cache.impl.nearcache.NearCachedClientCacheProxy;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.nearcache.impl.invalidation.MetaDataGenerator;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import static uk.org.hecaton.cache.CacheTestSupport.createClientCachingProvider;
import static uk.org.hecaton.cache.CacheTestSupport.createServerCachingProvider;
import static uk.org.hecaton.config.MaxSizePolicy.ENTRY_COUNT;
import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static java.lang.Integer.MAX_VALUE;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("WeakerAccess")
public class ClientCacheMetaDataGeneratorTest extends HecatonTestSupport {

    private static final String CACHE_NAME = "CacheMetaDataGeneratorTest";
    private static final String PREFIXED_CACHE_NAME = "/hz/" + CACHE_NAME;

    private final TestHecatonFactory factory = new TestHecatonFactory();

    private ClientConfig clientConfig;
    private CacheConfig<Integer, Integer> cacheConfig;

    private ICache<Integer, Integer> serverCache;
    private HecatonInstance server;

    @Before
    public void setUp() {
        NearCacheConfig nearCacheConfig = getNearCacheConfig();

        clientConfig = getClientConfig()
                .addNearCacheConfig(nearCacheConfig);

        cacheConfig = getCacheConfig(CACHE_NAME);

        Config config = getConfig();
        server = factory.newHecatonInstance(config);

        CachingProvider provider = createServerCachingProvider(server);
        CacheManager serverCacheManager = provider.getCacheManager();

        serverCache = (ICache<Integer, Integer>) serverCacheManager.createCache(CACHE_NAME, cacheConfig);
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void destroying_cache_removes_related_metadata_when_near_cache_exists() {
        ICache<Integer, Integer> clientCache = createCacheFromNewClient();

        serverCache.put(1, 1);

        assertNotNull(getMetaDataGenerator(server).getSequenceGenerators().get(PREFIXED_CACHE_NAME));

        clientCache.destroy();

        assertNull(getMetaDataGenerator(server).getSequenceGenerators().get(PREFIXED_CACHE_NAME));
    }

    @Test
    public void destroying_cache_removes_related_metadata_when_near_cache_not_exists() {
        serverCache.put(1, 1);

        assertNull(getMetaDataGenerator(server).getSequenceGenerators().get(PREFIXED_CACHE_NAME));

        serverCache.destroy();

        assertNull(getMetaDataGenerator(server).getSequenceGenerators().get(PREFIXED_CACHE_NAME));
    }

    @Override
    protected Config getConfig() {
        return getBaseConfig();
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    protected CacheConfig<Integer, Integer> getCacheConfig(String cacheName) {
        EvictionConfig evictionConfig = new EvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);

        return new CacheConfig<Integer, Integer>()
                .setName(cacheName)
                .setEvictionConfig(evictionConfig);
    }

    protected NearCacheConfig getNearCacheConfig() {
        return new NearCacheConfig(CACHE_NAME)
                .setInvalidateOnChange(true);
    }

    private ICache<Integer, Integer> createCacheFromNewClient() {
        HecatonClientProxy client = (HecatonClientProxy) factory.newHecatonClient(clientConfig);
        CachingProvider clientCachingProvider = createClientCachingProvider(client);

        CacheManager cacheManager = clientCachingProvider.getCacheManager();
        ICache<Integer, Integer> cache = (ICache<Integer, Integer>) cacheManager.createCache(CACHE_NAME, cacheConfig);

        assertInstanceOf(NearCachedClientCacheProxy.class, cache);

        return cache;
    }

    private static MetaDataGenerator getMetaDataGenerator(HecatonInstance member) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        CacheService service = nodeEngineImpl.getService(CacheService.SERVICE_NAME);
        CacheEventHandler cacheEventHandler = service.getCacheEventHandler();
        return cacheEventHandler.getMetaDataGenerator();
    }
}
