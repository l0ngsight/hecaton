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

package classloading;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientUserCodeDeploymentConfig;
import uk.org.hecaton.client.impl.spi.impl.discovery.ClientDiscoverySpiTest;
import uk.org.hecaton.config.DiscoveryStrategyConfig;
import uk.org.hecaton.config.SerializationConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.nio.serialization.HecatonSerializationException;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Set;

import static classloading.ThreadLeakTestUtils.assertHecatonThreadShutdown;
import static classloading.ThreadLeakTestUtils.getThreads;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class ThreadLeakClientTest {
    private int zeroTimeout = 0;

    @After
    public void shutdownInstances() {
        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
    }

    @Test
    public void testThreadLeak() {
        Set<Thread> testStartThreads = getThreads();
        HecatonInstance member = Hecaton.newHecatonInstance();
        HecatonInstance client = HecatonClient.newHecatonClient();

        client.shutdown();
        member.shutdown();

        assertHecatonThreadShutdown(testStartThreads);
    }

    @Test
    public void testThreadLeak_withoutCluster() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);

        Set<Thread> testStartThreads = getThreads();
        HecatonInstance client = HecatonClient.newHecatonClient(clientConfig);
        client.shutdown();
        assertHecatonThreadShutdown(testStartThreads);
    }

    @Test(expected = IllegalStateException.class)
    public void testThreadLeakWhenClientCanNotStart() {
        Set<Thread> testStartThreads = getThreads();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig()
                .getConnectionRetryConfig()
                .setClusterConnectTimeoutMillis(zeroTimeout);

        try {
            HecatonClient.newHecatonClient(clientConfig);
        } finally {
            assertHecatonThreadShutdown(testStartThreads);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testThreadLeakWhenClientCanNotStartDueToAuthenticationError() {
        Hecaton.newHecatonInstance();
        ClientConfig config = new ClientConfig();
        config.setClusterName("invalid cluster");
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(zeroTimeout);
        Set<Thread> testStartThreads = getThreads();
        try {
            HecatonClient.newHecatonClient(config);
        } finally {
            Hecaton.shutdownAll();
            assertHecatonThreadShutdown(testStartThreads);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testThreadLeakWhenClientCanNotConstructDueToNoMemberDiscoveryStrategyConfig() {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().getDiscoveryConfig().addDiscoveryStrategyConfig(
                new DiscoveryStrategyConfig(new ClientDiscoverySpiTest.NoMemberDiscoveryStrategyFactory(),
                        Collections.emptyMap()));
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(zeroTimeout);
        Set<Thread> testStartThreads = getThreads();
        try {
            HecatonClient.newHecatonClient(config);
        } finally {
            assertHecatonThreadShutdown(testStartThreads);
        }
    }

    @Test(expected = HecatonException.class)
    public void testThreadLeakWhenClientCanNotStartDueToIncorrectUserCodeDeploymentClass() {
        Hecaton.newHecatonInstance();

        ClientConfig config = new ClientConfig();
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass("invalid.class.test");
        config.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig.setEnabled(true));
        Set<Thread> testStartThreads = getThreads();
        try {
            HecatonClient.newHecatonClient(config);
        } finally {
            Hecaton.shutdownAll();
            assertHecatonThreadShutdown(testStartThreads);
        }
    }

    @Test(expected = HecatonSerializationException.class)
    public void testThreadLeakWhenClientCanNotConstructDueToIncorrectSerializationServiceFactoryClassName() {
        ClientConfig config = new ClientConfig();
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.addDataSerializableFactoryClass(5, "invalid.factory");
        config.setSerializationConfig(serializationConfig);
        Set<Thread> testStartThreads = getThreads();
        try {
            HecatonClient.newHecatonClient(config);
        } finally {
            assertHecatonThreadShutdown(testStartThreads);
        }
    }
}
