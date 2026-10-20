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

package uk.org.hecaton.client;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientNetworkConfig;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.client.test.TestAwareClientFactory;
import uk.org.hecaton.client.util.ConfigRoutingUtil;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.IExecutorService;
import uk.org.hecaton.core.OperationTimeoutException;
import uk.org.hecaton.spi.exception.RetryableHecatonException;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Tests in this class intentionally use real network.
 */
@RunWith(HecatonParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class})
public class ClientTimeoutTest {

    @Parameterized.Parameter
    public RoutingMode routingMode;

    @Parameterized.Parameters(name = "{index}: routingMode={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(RoutingMode.SINGLE_MEMBER, RoutingMode.ALL_MEMBERS);
    }

    private final TestAwareClientFactory factory = new TestAwareClientFactory();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test(timeout = 20000, expected = IllegalStateException.class)
    public void testTimeoutToOutsideNetwork() {
        ClientConfig clientConfig = newClientConfig();
        clientConfig.setClusterName("dev");

        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        networkConfig.addAddress("8.8.8.8:5701");
        // Do only one connection-attempt
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(1000);
        // Timeout connection-attempt after 1000 millis
        networkConfig.setConnectionTimeout(1000);

        HecatonInstance client = factory.newHecatonClient(clientConfig);
        client.getList("test");
    }

    @Test
    public void testConnectionTimeout_withIntMax() {
        testConnectionTimeout(Integer.MAX_VALUE);
    }

    @Test
    public void testConnectionTimeout_withZeroValue() {
        testConnectionTimeout(0);
    }

    public void testConnectionTimeout(int timeoutInMillis) {
        //Should work without throwing exception.
        ClientConfig clientConfig = newClientConfig();
        clientConfig.getNetworkConfig().setConnectionTimeout(timeoutInMillis);
        factory.newHecatonInstance(new Config());
        factory.newHecatonClient(clientConfig);
    }

    @Test(expected = OperationTimeoutException.class)
    public void testInvocationTimeOut() throws Throwable {
        ClientConfig clientConfig = newClientConfig();
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "0");
        factory.newHecatonInstance(new Config());
        HecatonInstance client = factory.newHecatonClient(clientConfig);
        IExecutorService executorService = client.getExecutorService("test");
        Future<Boolean> future = executorService.submit(new RetryableExceptionThrowingCallable());
        try {
            future.get();
        } catch (InterruptedException e) {
            //ignored
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    private static class RetryableExceptionThrowingCallable implements Callable<Boolean>, Serializable {
        @Override
        public Boolean call() {
            throw new RetryableHecatonException();
        }
    }

    private ClientConfig newClientConfig() {
        return ConfigRoutingUtil.newClientConfig(routingMode);
    }
}
