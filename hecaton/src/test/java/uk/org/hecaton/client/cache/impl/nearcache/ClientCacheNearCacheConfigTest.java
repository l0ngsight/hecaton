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

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.test.HecatonParallelClassRunner;
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
import static uk.org.hecaton.client.config.XmlClientConfigBuilderTest.buildConfig;
import static uk.org.hecaton.config.MaxSizePolicy.ENTRY_COUNT;
import static java.lang.Integer.MAX_VALUE;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheNearCacheConfigTest {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @Before
    public void setUp() {
        factory.newHecatonInstance();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test(expected = IllegalArgumentException.class)
    public void native_backed_nearCache_throws_illegalArgumentException_whenNoNativeConfigAvailable() {
        String xml = "<hecaton-client xmlns=\"https://hecaton.org.uk/schema/client-config\">"
                + "<near-cache name=\"test\">"
                + "<in-memory-format>NATIVE</in-memory-format>"
                + "</near-cache></hecaton-client>";

        ClientConfig clientConfig = buildConfig(xml);
        HecatonClientProxy client = (HecatonClientProxy) factory.newHecatonClient(clientConfig);
        CachingProvider clientCachingProvider = createClientCachingProvider(client);
        CacheManager cacheManager = clientCachingProvider.getCacheManager();
        cacheManager.createCache("test", newCacheConfig());
    }

    private CacheConfig newCacheConfig() {
        CacheConfig cacheConfig = new CacheConfig();
        cacheConfig.getEvictionConfig()
                .setMaxSizePolicy(ENTRY_COUNT)
                .setSize(MAX_VALUE);
        return cacheConfig;
    }
}
