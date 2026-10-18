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

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;

import static uk.org.hecaton.cache.CacheTestSupport.createClientCachingProvider;
import static org.junit.Assert.assertNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheProxyTest extends ClientTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void clusterRestart_proxyRemainsUsableOnClient() {
        HecatonInstance instance = factory.newHecatonInstance();
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HecatonInstance client = factory.newHecatonClient(clientConfig);

        CachingProvider cachingProvider = createClientCachingProvider(client);

        CacheManager cacheManager = cachingProvider.getCacheManager();

        CompleteConfiguration<String, String> config =
                new MutableConfiguration<String, String>()
                        .setTypes(String.class, String.class);

        javax.cache.Cache<String, String> cache = cacheManager.createCache("example", config);
        //restarting cluster
        ReconnectListener reconnectListener = new ReconnectListener();
        client.getLifecycleService().addLifecycleListener(reconnectListener);

        instance.shutdown();
        factory.newHecatonInstance();

        assertOpenEventually(reconnectListener.reconnectedLatch);

        //expected to work without throwing exception
        assertNull(cache.get("key"));

    }

    @Test
    public void clientRestart_proxyRemainsUsableOnClient() {
        factory.newHecatonInstance();

        HecatonInstance client = factory.newHecatonClient();
        CachingProvider cachingProvider = createClientCachingProvider(client);
        CacheManager cacheManager = cachingProvider.getCacheManager();
        CompleteConfiguration<String, String> config =
                new MutableConfiguration<String, String>()
                        .setTypes(String.class, String.class);
        String cacheName = "example";
        cacheManager.createCache(cacheName, config);

        //restarting client and getting already created cache
        client.shutdown();
        client = factory.newHecatonClient();
        cachingProvider = createClientCachingProvider(client);
        cacheManager = cachingProvider.getCacheManager();
        javax.cache.Cache<String, String> cache = cacheManager.getCache(cacheName);

        //expected to work without throwing exception
        assertNull(cache.get("key"));
    }
}

