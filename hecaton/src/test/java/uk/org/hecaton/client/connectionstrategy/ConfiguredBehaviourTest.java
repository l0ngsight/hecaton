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

package uk.org.hecaton.client.connectionstrategy;

import uk.org.hecaton.client.HecatonClientNotActiveException;
import uk.org.hecaton.client.HecatonClientOfflineException;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientUserCodeDeploymentConfig;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.LifecycleEvent;
import uk.org.hecaton.core.LifecycleListener;
import uk.org.hecaton.internal.util.FilteringClassLoader;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import usercodedeployment.IncrementingEntryProcessor;

import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.client.config.ClientConnectionStrategyConfig.ReconnectMode.ASYNC;
import static uk.org.hecaton.client.config.ClientConnectionStrategyConfig.ReconnectMode.OFF;
import static uk.org.hecaton.core.LifecycleEvent.LifecycleState.CLIENT_CONNECTED;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfiguredBehaviourTest extends ClientTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test(expected = HecatonClientOfflineException.class)
    public void testAsyncStartTrueNoCluster() {

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        client.getMap(randomMapName());
    }

    @Test(expected = HecatonClientNotActiveException.class)
    public void testAsyncStartTrueNoCluster_thenShutdown() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        client.shutdown();

        client.getMap(randomMapName());
    }


    @Test
    public void testAsyncStartTrue() {
        CountDownLatch connectedLatch = new CountDownLatch(1);

        ClientConfig clientConfig = new ClientConfig();

        // reserve a member address so that the client can be configured to look for it
        Address memberAddress = hecatonFactory.nextAddress();

        // trying 8.8.8.8 address will delay the initial connection since no such server exist
        clientConfig.getNetworkConfig().addAddress("8.8.8.8", memberAddress.getHost() + ":" + memberAddress.getPort());
        clientConfig.addListenerConfig(new ListenerConfig((LifecycleListener) event -> {
            if (event.getState() == CLIENT_CONNECTED) {
                connectedLatch.countDown();
            }
        }));
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);

        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        assertTrue(client.getLifecycleService().isRunning());

        hecatonFactory.newHecatonInstance(memberAddress, null);

        assertOpenEventually(connectedLatch);

        assertTrueEventually(() -> {
            try {
                client.getMap(randomMapName());
            } catch (Exception e) {
                fail();
            }
        });
    }

    @Test
    public void testAsyncStartTrueShouldNotBlock_whenThereIsClientStateToSend() {
        Config config = new Config();
        FilteringClassLoader filteringCL = new FilteringClassLoader(singletonList("usercodedeployment"), null);
        config.setClassLoader(filteringCL);
        config.getUserCodeDeploymentConfig().setEnabled(true);
        hecatonFactory.newHecatonInstance(config);
        hecatonFactory.newHecatonInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(-1);
        ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig = new ClientUserCodeDeploymentConfig();
        clientUserCodeDeploymentConfig.addClass(IncrementingEntryProcessor.class);
        clientUserCodeDeploymentConfig.setEnabled(true);
        clientConfig.setUserCodeDeploymentConfig(clientUserCodeDeploymentConfig);
        clientConfig.getConnectionStrategyConfig().setAsyncStart(true);

        CountDownLatch connectedLatch = new CountDownLatch(1);
        clientConfig.addListenerConfig(new ListenerConfig((LifecycleListener) event -> {
            if (event.getState() == CLIENT_CONNECTED) {
                connectedLatch.countDown();
            }
        }));

        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        assertOpenEventually(connectedLatch);

        // The following logic verifies user code deployment works
        int keyCount = 100;
        IMap<Integer, Integer> map = client.getMap(randomName());

        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0);
        }

        int incrementCount = 5;
        //doing the call a few times so that the invocation can be done on different members
        for (int i = 0; i < incrementCount; i++) {
            map.executeOnEntries(new IncrementingEntryProcessor());
        }

        for (int i = 0; i < keyCount; i++) {
            assertEquals(incrementCount, (int) map.get(i));
        }
    }

    @Test(expected = HecatonClientNotActiveException.class)
    public void testReconnectModeOFFSingleMember() {
        HecatonInstance hecatonInstance = hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setReconnectMode(OFF);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(event -> {
            if (LifecycleEvent.LifecycleState.SHUTDOWN == event.getState()) {
                shutdownLatch.countDown();
            }
        });

        // no exception at this point
        IMap<Integer, Integer> map = client.getMap(randomMapName());
        map.put(1, 5);

        hecatonInstance.shutdown();
        assertOpenEventually(shutdownLatch);

        map.put(1, 5);
    }

    @Test(expected = HecatonClientNotActiveException.class)
    public void testReconnectModeOFFTwoMembers() {
        hecatonFactory.newInstances(getConfig(), 2);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().setReconnectMode(OFF);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        CountDownLatch shutdownLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(event -> {
            if (LifecycleEvent.LifecycleState.SHUTDOWN == event.getState()) {
                shutdownLatch.countDown();
            }
        });

        // no exception at this point
        IMap<Integer, Integer> map = client.getMap(randomMapName());
        map.put(1, 5);

        hecatonFactory.shutdownAllMembers();
        assertOpenEventually(shutdownLatch);

        map.put(1, 5);
    }

    @Test(expected = HecatonClientOfflineException.class)
    public void testReconnectModeASYNCSingleMemberInitiallyOffline() {
        ClientConfig clientConfig = new ClientConfig();

        HecatonInstance hecatonInstance = hecatonFactory.newHecatonInstance();

        clientConfig.getConnectionStrategyConfig().setReconnectMode(ASYNC);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        CountDownLatch disconnectedLatch = new CountDownLatch(1);
        client.getLifecycleService().addLifecycleListener(event -> {
            if (LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED == event.getState()) {
                disconnectedLatch.countDown();
            }
        });

        hecatonInstance.shutdown();
        assertOpenEventually(disconnectedLatch);
        client.getMap(randomMapName());
    }

    @Test
    public void testReconnectModeASYNCSingleMember() {
        hecatonFactory.newHecatonInstance();

        CountDownLatch connectedLatch = new CountDownLatch(1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addListenerConfig(new ListenerConfig((LifecycleListener) event -> {
            if (event.getState() == CLIENT_CONNECTED) {
                connectedLatch.countDown();
            }
        }));
        clientConfig.getConnectionStrategyConfig().setReconnectMode(ASYNC);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        assertTrue(client.getLifecycleService().isRunning());

        assertOpenEventually(connectedLatch);

        client.getMap(randomMapName());
    }

    @Test
    public void testReconnectModeASYNC_clusterDown_clientGetsOfflineExcption() {
        HecatonInstance member1 = hecatonFactory.newHecatonInstance();
        HecatonInstance member2 = hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.getConnectionStrategyConfig().setReconnectMode(ASYNC);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        IMap<Object, Object> map = client.getMap(randomMapName());

        member1.shutdown();
        member2.shutdown();

        assertTrue(client.getLifecycleService().isRunning());
        for (int i = 0; i < 100; i++) {
            try {
                map.get(randomString());
                fail("map.get should throw HecatonClientOfflineException");
            } catch (HecatonClientOfflineException ignored) {
            }
        }
    }

    @Test
    public void testReconnectModeASYNCSingleMemberStartLate() {
        CountDownLatch initialConnectionLatch = new CountDownLatch(1);
        CountDownLatch reconnectedLatch = new CountDownLatch(1);

        HecatonInstance hecatonInstance = hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.addListenerConfig(new ListenerConfig((LifecycleListener) event -> {
            if (event.getState() == CLIENT_CONNECTED) {
                initialConnectionLatch.countDown();
            }
        }));
        clientConfig.getConnectionStrategyConfig().setReconnectMode(ASYNC);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        assertOpenEventually(initialConnectionLatch);

        hecatonInstance.shutdown();

        client.getLifecycleService().addLifecycleListener(event -> {
            if (event.getState() == CLIENT_CONNECTED) {
                reconnectedLatch.countDown();
            }
        });

        hecatonFactory.newHecatonInstance();

        assertTrue(client.getLifecycleService().isRunning());
        assertOpenEventually(reconnectedLatch);

        client.getMap(randomMapName());
    }

    @Test
    public void testReconnectModeASYNCTwoMembers() {
        hecatonFactory.newInstances(getConfig(), 2);

        CountDownLatch connectedLatch = new CountDownLatch(1);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addListenerConfig(new ListenerConfig((LifecycleListener) event -> {
            if (event.getState() == CLIENT_CONNECTED) {
                connectedLatch.countDown();
            }
        }));
        clientConfig.getConnectionStrategyConfig().setReconnectMode(ASYNC);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        assertTrue(client.getLifecycleService().isRunning());

        assertOpenEventually(connectedLatch);

        IMap<Integer, Integer> map = client.getMap(randomMapName());
        map.put(1, 5);

        hecatonFactory.shutdownAllMembers();

        HecatonInstance[] instances = hecatonFactory.newInstances(getConfig(), 2);

        assertTrueEventually(() -> {
            Set<Member> actualMembers = client.getCluster().getMembers();
            Set<Member> expectedMembers = instances[0].getCluster().getMembers();
            assertEquals(expectedMembers, actualMembers);
        });

        assertTrueEventually(() -> {
            try {
                map.get(1);
            } catch (HecatonClientOfflineException e) {
                fail();
            }
        });
    }
}
