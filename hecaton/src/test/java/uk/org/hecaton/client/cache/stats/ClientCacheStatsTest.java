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

package uk.org.hecaton.client.cache.stats;

import uk.org.hecaton.cache.CacheStatistics;
import uk.org.hecaton.cache.ICache;
import uk.org.hecaton.cache.stats.CacheStatsTest;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.cache.spi.CachingProvider;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCacheStatsTest extends CacheStatsTest {

    @Parameter
    public boolean nearCacheEnabled;

    private final TestHecatonFactory instanceFactory = new TestHecatonFactory();
    private HecatonInstance client;

    @Parameters(name = "nearCached:{0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{
                Boolean.TRUE,
                Boolean.FALSE,
        });
    }

    @Override
    protected void onSetup() {
        super.onSetup();
        getHecatonInstance();
        ClientConfig clientConfig = createClientConfig();
        client = instanceFactory.newHecatonClient(clientConfig);
    }

    @Override
    protected void onTearDown() {
        super.onTearDown();
        instanceFactory.shutdownAll();
    }

    @Override
    protected HecatonInstance getHecatonInstance() {
        return instanceFactory.newHecatonInstance(createConfig());
    }

    @Override
    protected CachingProvider getCachingProvider() {
        return createClientCachingProvider(client);
    }

    protected ClientConfig createClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        if (nearCacheEnabled) {
            clientConfig.addNearCacheConfig(new NearCacheConfig("*"));
        }
        return clientConfig;
    }

    @Override
    @Test
    public void testOwnedEntryCountWhenThereIsNoBackup() {
        assertThrows(UnsupportedOperationException.class, super::testOwnedEntryCountWhenThereIsNoBackup);
    }

    @Override
    @Test
    public void testOwnedEntryCountWhenThereAreBackupsOnStaticCluster() {
        assertThrows(UnsupportedOperationException.class, super::testOwnedEntryCountWhenThereAreBackupsOnStaticCluster);
    }

    @Override
    @Test
    public void testOwnedEntryCountWhenThereAreBackupsOnDynamicCluster() {
        assertThrows(UnsupportedOperationException.class, super::testOwnedEntryCountWhenThereAreBackupsOnDynamicCluster);
    }

    @Override
    @Test
    public void testExpirations() {
        assertThrows(UnsupportedOperationException.class, super::testExpirations);
    }

    @Override
    @Test
    public void testEvictions() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        assertThrows(UnsupportedOperationException.class, stats::getCacheEvictions);
    }

    @Override
    @Test
    public void testNearCacheStats_availableWhenEnabled() {
        if (nearCacheEnabled) {
            testNearCacheStats_whenNearCacheEnabled();
        } else {
            assertThrows(UnsupportedOperationException.class, this::testNearCacheStats_whenNearCacheDisabled);
        }
    }

    // throws UnsupportedOperationException
    private void testNearCacheStats_whenNearCacheDisabled() {
        ICache<Integer, String> cache = createCache();
        CacheStatistics stats = cache.getLocalCacheStatistics();

        stats.getNearCacheStatistics();
    }

    private void testNearCacheStats_whenNearCacheEnabled() {
        String cacheName = randomName();
        CacheConfig cacheConfig = createCacheConfig();
        cacheConfig.setName(cacheName);
        ClientConfig clientConfig = ((HecatonClientProxy) client).getClientConfig();
        clientConfig.addNearCacheConfig(new NearCacheConfig().setName(cacheName));
        ICache<Integer, String> cache = createCache(cacheName, cacheConfig);
        CacheStatistics stats = cache.getLocalCacheStatistics();

        assertNotNull(stats.getNearCacheStatistics());
    }
}
