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
import uk.org.hecaton.client.config.ClientConnectionStrategyConfig;
import uk.org.hecaton.client.config.ClientNetworkConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.connection.AddressProvider;
import uk.org.hecaton.client.impl.connection.Addresses;
import uk.org.hecaton.client.impl.connection.ClientConnectionManager;
import uk.org.hecaton.client.impl.connection.tcp.TcpClientConnectionManager;
import uk.org.hecaton.client.impl.management.ClientConnectionProcessListenerRegistry;
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.client.util.AddressHelper;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.listener.EntryAddedListener;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientRegressionWithRealNetworkTest extends ClientTestSupport {

    @After
    public void cleanUp() {
        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
    }

    @Test
    @Category(QuickTest.class)
    public void testClientPortConnection() {
        String clusterName1 = randomString();
        Config config1 = new Config();
        config1.setClusterName(clusterName1);
        config1.getNetworkConfig().setPort(5701);
        HecatonInstance instance1 = Hecaton.newHecatonInstance(config1);
        instance1.getMap("map").put("key", "value");

        String clusterName2 = randomString();
        Config config2 = new Config();
        config2.setClusterName(clusterName2);
        config2.getNetworkConfig().setPort(5702);
        Hecaton.newHecatonInstance(config2);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(clusterName2);
        HecatonInstance client = HecatonClient.newHecatonClient(clientConfig);

        IMap<Object, Object> map = client.getMap("map");
        assertNull(map.put("key", "value"));
        assertEquals(1, map.size());
    }

    @Test
    public void testClientConnectionBeforeServerReady() {
        String clusterName = randomString();
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.submit(() -> {
            Config config = new Config();
            config.setClusterName(clusterName);
            Hecaton.newHecatonInstance(config);
        });

        CountDownLatch clientLatch = new CountDownLatch(1);
        executorService.submit(() -> {
            ClientConfig config = new ClientConfig();
            config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
            config.setClusterName(clusterName);
            HecatonClient.newHecatonClient(config);
            clientLatch.countDown();
        });

        assertOpenEventually(clientLatch);
    }

    @Test
    public void testConnectionCountAfterClientReconnect_memberHostname_clientIp() {
        testConnectionCountAfterClientReconnect("localhost", "127.0.0.1");
    }

    @Test
    public void testConnectionCountAfterClientReconnect_memberHostname_clientHostname() {
        testConnectionCountAfterClientReconnect("localhost", "localhost");
    }

    @Test
    public void testConnectionCountAfterClientReconnect_memberIp_clientIp() {
        testConnectionCountAfterClientReconnect("127.0.0.1", "127.0.0.1");
    }

    @Test
    public void testConnectionCountAfterClientReconnect_memberIp_clientHostname() {
        testConnectionCountAfterClientReconnect("127.0.0.1", "localhost");
    }

    private void testConnectionCountAfterClientReconnect(String memberAddress, String clientAddress) {
        String clusterName = randomString();
        Config config = new Config();
        config.setClusterName(clusterName);
        config.getNetworkConfig().setPublicAddress(memberAddress);
        HecatonInstance hecatonInstance = Hecaton.newHecatonInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress(clientAddress);
        clientConfig.setClusterName(clusterName);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);

        HecatonInstance client = HecatonClient.newHecatonClient(clientConfig);
        HecatonClientInstanceImpl clientInstanceImpl = getHecatonClientInstanceImpl(client);
        ClientConnectionManager connectionManager = clientInstanceImpl.getConnectionManager();

        assertTrueEventually(() -> assertEquals(1, connectionManager.getActiveConnections().size()));

        ReconnectListener reconnectListener = new ReconnectListener();
        client.getLifecycleService().addLifecycleListener(reconnectListener);

        hecatonInstance.shutdown();
        assertOpenEventually(reconnectListener.disconnectedLatch);
        Hecaton.newHecatonInstance(config);

        assertOpenEventually(reconnectListener.reconnectedLatch);
        assertEquals(1, connectionManager.getActiveConnections().size());
    }

    @Test
    public void testListenersAfterClientDisconnected_memberHostname_clientIp() {
        testListenersAfterClientDisconnected("localhost", "127.0.0.1");
    }

    @Test
    public void testListenersAfterClientDisconnected_memberHostname_clientHostname() {
        testListenersAfterClientDisconnected("localhost", "localhost");
    }

    @Test
    public void testListenersAfterClientDisconnected_memberIp_clientIp() {
        testListenersAfterClientDisconnected("127.0.0.1", "127.0.0.1");
    }

    @Test
    public void testListenersAfterClientDisconnected_memberIp_clientHostname() {
        testListenersAfterClientDisconnected("127.0.0.1", "localhost");
    }

    private void testListenersAfterClientDisconnected(String memberAddress, String clientAddress) {
        String clusterName = randomString();
        Config config = new Config();
        int heartBeatSeconds = 6;
        config.getNetworkConfig().setPublicAddress(memberAddress);
        config.setProperty(ClusterProperty.CLIENT_HEARTBEAT_TIMEOUT_SECONDS.getName(), Integer.toString(heartBeatSeconds));
        config.setClusterName(clusterName);
        HecatonInstance hecatonInstance = Hecaton.newHecatonInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(clusterName);
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        networkConfig.addAddress(clientAddress);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HecatonInstance client = HecatonClient.newHecatonClient(clientConfig);
        IMap<Integer, Integer> map = client.getMap("test");

        AtomicInteger eventCount = new AtomicInteger();

        map.addEntryListener((EntryAddedListener<Object, Object>) event -> eventCount.incrementAndGet(), false);

        assertTrueEventually(() -> {
            HecatonClientInstanceImpl clientInstanceImpl = getHecatonClientInstanceImpl(client);
            int size = clientInstanceImpl.getConnectionManager().getActiveConnections().size();
            assertEquals(1, size);

        });

        hecatonInstance.shutdown();
        sleepAtLeastSeconds(2 * heartBeatSeconds);
        Hecaton.newHecatonInstance(config);

        assertTrueEventually(() -> {
            map.remove(1);
            map.put(1, 2);
            assertNotEquals(0, eventCount.get());
        });
    }

    @Test
    public void testOperationsContinueWhenClientDisconnected_reconnectModeAsync() {
        testOperationsContinueWhenClientDisconnected(ClientConnectionStrategyConfig.ReconnectMode.ASYNC);
    }

    @Test
    public void testOperationsContinueWhenClientDisconnected_reconnectModeOn() {
        testOperationsContinueWhenClientDisconnected(ClientConnectionStrategyConfig.ReconnectMode.ON);
    }

    private void testOperationsContinueWhenClientDisconnected(ClientConnectionStrategyConfig.ReconnectMode reconnectMode) {
        String clusterName = randomString();
        Config config = new Config();
        config.setClusterName(clusterName);
        HecatonInstance instance1 = Hecaton.newHecatonInstance(config);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(clusterName);
        clientConfig.getConnectionStrategyConfig().setReconnectMode(reconnectMode);
        AtomicBoolean waitFlag = new AtomicBoolean();
        CountDownLatch testFinished = new CountDownLatch(1);
        AddressProvider addressProvider = new AddressProvider() {
            @Override
            public Addresses loadAddresses(ClientConnectionProcessListenerRegistry listenerRunner) {
                if (waitFlag.get()) {
                    try {
                        testFinished.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                return AddressHelper.getSocketAddresses("127.0.0.1", listenerRunner);
            }

            @Override
            public Address translate(Address address) {
                return address;
            }

            @Override
            public Address translate(Member member) throws Exception {
                return member.getAddress();
            }
        };
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        clientConfig.setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), "3");
        HecatonInstance client = HecatonClientUtil.newHecatonClient(clientConfig, addressProvider);

        HecatonInstance instance2 = Hecaton.newHecatonInstance(config);

        warmUpPartitions(instance1, instance2);
        String keyOwnedBy2 = generateKeyOwnedBy(instance2);
        makeSureConnectedToServers(client, 2);

        IMap<Object, Object> clientMap = client.getMap("test");

        //we are closing a connection and making sure It is not established ever again
        waitFlag.set(true);
        UUID memberUUID = instance1.getLocalEndpoint().getUuid();
        instance1.shutdown();

        makeSureDisconnectedFromServer(client, memberUUID);
        //we expect these operations to run without throwing exception, since they are done on live instance.
        clientMap.put(keyOwnedBy2, 1);
        assertEquals(1, clientMap.get(keyOwnedBy2));

        testFinished.countDown();
    }

    @Test
    public void testNioChannelLeakTest() {
        ClientConfig config = new ClientConfig();
        config.getConnectionStrategyConfig().setAsyncStart(true).
                setReconnectMode(ClientConnectionStrategyConfig.ReconnectMode.ASYNC)
                .getConnectionRetryConfig().setInitialBackoffMillis(1).setClusterConnectTimeoutMillis(1000);
        HecatonInstance client = HecatonClient.newHecatonClient(config);
        HecatonClientInstanceImpl clientInstanceImpl = getHecatonClientInstanceImpl(client);
        TcpClientConnectionManager connectionManager = (TcpClientConnectionManager) clientInstanceImpl.getConnectionManager();
        sleepSeconds(2);
        assertTrueEventually(() -> assertEquals(0, connectionManager.getNetworking().getChannels().size()));
        client.shutdown();
    }
}
