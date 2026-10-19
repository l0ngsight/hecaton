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

import uk.org.hecaton.cache.CacheListenerTest;
import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.cache.impl.HecatonClientCachingProvider;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;
import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.cache.CacheTestSupport.createClientCachingProvider;
import static org.junit.Assert.assertEquals;

@Category(SlowTest.class)
public class CacheClientListenerTest extends CacheListenerTest {
    @Before
    @After
    public void cleanup() {
        HecatonClient.shutdownAll();
        HecatonInstanceFactory.terminateAll();
    }

    @Override
    protected CachingProvider getCachingProvider() {
        Config config = new Config();
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig().setEnabled(false);
        joinConfig.getTcpIpConfig().setEnabled(false);

        Hecaton.newHecatonInstance(config);
        Hecaton.newHecatonInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1");

        hecatonInstance = HecatonClient.newHecatonClient(clientConfig);

        return createClientCachingProvider(hecatonInstance);
    }

    @Test
    public void testEntryListenerUsingMemberConfig() {
        System.setProperty("hecaton.config", "classpath:hecaton-cache-entrylistener-test.xml");
        Hecaton.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1");

        HecatonInstance client = HecatonClient.newHecatonClient(clientConfig);

        HecatonClientCachingProvider cachingProvider = createClientCachingProvider(client);

        CacheManager cacheManager = cachingProvider.getCacheManager();

        Cache<String, String> cache = cacheManager.getCache("entrylistenertestcache");

        cache.put("foo", "bar");

        HecatonInstance client2 = HecatonClient.newHecatonClient(clientConfig);

        HecatonClientCachingProvider cachingProvider2 = createClientCachingProvider(client2);

        CacheManager cacheManager2 = cachingProvider2.getCacheManager();

        Cache<String, String> cache2 = cacheManager2.getCache("entrylistenertestcache");

        client.shutdown();

        // sleep enough so that the put entry is expired
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Trigger expiry event
        cache2.get("foo");

        final CountDownLatch expiredLatch = ClientCacheEntryExpiredLatchCountdownListener.getExpiredLatch();
        assertCountEventually("The expired event should only be received one time", 1, expiredLatch, 3);

        assertTrueAllTheTime(() -> assertEquals("Expired event is received more than once", 1, expiredLatch.getCount()), 3);
    }
}
