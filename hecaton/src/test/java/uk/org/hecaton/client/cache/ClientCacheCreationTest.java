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

import uk.org.hecaton.cache.CacheCreationTest;
import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.cache.impl.HecatonClientCachingProvider;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.ExecutionCallback;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonOverloadException;
import uk.org.hecaton.core.IExecutorService;
import uk.org.hecaton.core.OperationTimeoutException;
import uk.org.hecaton.executor.ExecutorServiceTestSupport;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.CacheManager;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;
import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.cache.CacheTestSupport.createClientCachingProvider;
import static java.util.Collections.singletonList;
import static org.junit.Assert.fail;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientCacheCreationTest extends CacheCreationTest {

    @Override
    protected CachingProvider createCachingProvider(Config hzConfig) {
        Hecaton.newHecatonInstance(hzConfig);
        ClientConfig clientConfig = null;
        if (hzConfig != null) {
            clientConfig = new ClientConfig();
            clientConfig.setClusterName(hzConfig.getClusterName());
            clientConfig.getNetworkConfig().setAddresses(singletonList("127.0.0.1"));
        }
        return createClientCachingProvider(HecatonClient.newHecatonClient(clientConfig));
    }

    @Test(expected = OperationTimeoutException.class)
    public void createSingleCache_whenMemberDown_throwsOperationTimeoutException() {
        HecatonInstance hecatonInstance = Hecaton.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "2");
        HecatonInstance client = HecatonClient.newHecatonClient(clientConfig);
        HecatonClientCachingProvider cachingProvider = createClientCachingProvider(client);
        CacheManager cacheManager = cachingProvider.getCacheManager();

        hecatonInstance.shutdown();
        MutableConfiguration configuration = new MutableConfiguration();
        cacheManager.createCache("xmlCache", configuration);
    }

    @Test
    public void createSingleCache_whenMemberBounce() throws InterruptedException {
        HecatonInstance hecatonInstance = Hecaton.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HecatonInstance client = HecatonClient.newHecatonClient(clientConfig);
        HecatonClientCachingProvider cachingProvider = createClientCachingProvider(client);
        final CacheManager cacheManager = cachingProvider.getCacheManager();

        hecatonInstance.shutdown();

        final CountDownLatch cacheCreated = new CountDownLatch(1);
        new Thread(() -> {
            MutableConfiguration configuration = new MutableConfiguration();
            cacheManager.createCache("xmlCache", configuration);
            cacheCreated.countDown();
        }).start();

        //leave some gap to let create cache to start and retry
        Thread.sleep(2000);
        Hecaton.newHecatonInstance();
        assertOpenEventually(cacheCreated);
    }

    @Test
    public void recreateCacheOnRestartedCluster_whenMaxConcurrentInvocationLow() {
        HecatonInstance hecatonInstance = Hecaton.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.MAX_CONCURRENT_INVOCATIONS.getName(), "1");
        // disable metrics collection (the periodic send statistics task may interfere with the test)
        clientConfig.getMetricsConfig().setEnabled(false);
        // disable backup acknowledgements (the backup listener registration may interfere with the test)
        clientConfig.setBackupAckToClientEnabled(false);
        HecatonInstance client = HecatonClient.newHecatonClient(clientConfig);
        HecatonClientCachingProvider cachingProvider = createClientCachingProvider(client);
        final CacheManager cacheManager = cachingProvider.getCacheManager();
        MutableConfiguration configuration = new MutableConfiguration();
        // ensure cache is created despite the low concurrent invocation limit
        assertTrueEventually(() -> {
            try {
                cacheManager.createCache("xmlCache", configuration);
            } catch (HecatonOverloadException e) {
                throw new AssertionError("Could not create cache due to "
                        + "low concurrent invocation count.");
            }
        });

        IExecutorService executorService = client.getExecutorService("exec");
        //keep the slot for one invocation to test if client can reconnect even if all slots are kept
        CountDownLatch testFinished = new CountDownLatch(1);
        executorService.submit(new ExecutorServiceTestSupport.SleepingTask(0),
                new ExecutionCallback<>() {
                    @Override
                    public void onResponse(Boolean response) {
                        try {
                            testFinished.await();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        try {
                            testFinished.await();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });

        hecatonInstance.shutdown();

        HecatonInstance instance = Hecaton.newHecatonInstance();
        assertTrueEventually(() -> {
            try {
                instance.getCacheManager().getCache("xmlCache");
            } catch (Exception e) {
                fail();
            }
        });
        testFinished.countDown();
    }

    @Test
    @Ignore("Only applicable for member-side HecatonInstance")
    @Override
    public void createInvalidCache_fromDeclarativeConfig_throwsException_fromHecatonInstanceCreation() {
    }

    @Override
    public void teardown() {
        super.teardown();
        HecatonClient.shutdownAll();
    }
}
