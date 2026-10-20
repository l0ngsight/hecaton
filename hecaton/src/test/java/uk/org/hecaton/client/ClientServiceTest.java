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
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.impl.spi.impl.ClientExecutionServiceImpl;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.core.EntryAdapter;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.LifecycleEvent;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.NightlyTest;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientServiceTest extends ClientTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void cleanup() {
        hecatonFactory.terminateAll();
    }

    @Test(expected = NullPointerException.class)
    public void testAddClientListener_whenListenerIsNull() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();

        ClientService clientService = instance.getClientService();
        clientService.addClientListener(null);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveClientListener_whenIdIsNull() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();

        ClientService clientService = instance.getClientService();
        clientService.removeClientListener(null);
    }

    @Test(timeout = 120000)
    public void testRemoveClientListener_whenListenerAlreadyRemoved() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();

        ClientService clientService = instance.getClientService();
        ClientListener clientListener = mock(ClientListener.class);
        UUID id = clientService.addClientListener(clientListener);

        // first time remove
        assertTrue(clientService.removeClientListener(id));

        // second time remove
        assertFalse(clientService.removeClientListener(id));
    }

    @Test(timeout = 120000)
    public void testRemoveClientListener_whenNonExistingId() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();

        ClientService clientService = instance.getClientService();

        assertFalse(clientService.removeClientListener(UUID.randomUUID()));
    }

    @Test(timeout = 120000)
    public void testNumberOfClients_afterUnAuthenticatedClient() {
        final HecatonInstance instance = hecatonFactory.newHecatonInstance();
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("wrongName");
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(2000);
        try {
            hecatonFactory.newHecatonClient(clientConfig);
        } catch (IllegalStateException ignored) {

        }

        assertEquals(0, instance.getClientService().getConnectedClients().size());
    }

    @Test(timeout = 120000)
    public void testNumberOfClients_afterUnAuthenticatedClient_withTwoNode() {
        final HecatonInstance instance1 = hecatonFactory.newHecatonInstance();
        final HecatonInstance instance2 = hecatonFactory.newHecatonInstance();
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("wrongName");
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(2000);
        try {
            hecatonFactory.newHecatonClient(clientConfig);
        } catch (IllegalStateException ignored) {

        }

        assertEquals(0, instance1.getClientService().getConnectedClients().size());
        assertEquals(0, instance2.getClientService().getConnectedClients().size());
    }


    @Test(timeout = 120000)
    @Category(NightlyTest.class)
    public void testNumberOfClients_afterUnAuthenticatedClient_withTwoNode_twoClient() {
        final HecatonInstance instance1 = hecatonFactory.newHecatonInstance();
        final HecatonInstance instance2 = hecatonFactory.newHecatonInstance();
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("wrongName");
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(2000);
        try {
            hecatonFactory.newHecatonClient(clientConfig);
        } catch (IllegalStateException ignored) {

        }
        try {
            hecatonFactory.newHecatonClient(clientConfig);
        } catch (IllegalStateException ignored) {

        }

        assertEquals(0, instance1.getClientService().getConnectedClients().size());
        assertEquals(0, instance2.getClientService().getConnectedClients().size());
    }


    @Test(timeout = 120000)
    public void testConnectedClients() {
        final HecatonInstance instance = hecatonFactory.newHecatonInstance();

        final HecatonInstance client1 = hecatonFactory.newHecatonClient();
        final HecatonInstance client2 = hecatonFactory.newHecatonClient();

        final ClientService clientService = instance.getClientService();
        final Collection<Client> connectedClients = clientService.getConnectedClients();
        assertEquals(2, connectedClients.size());

        final UUID uuid1 = client1.getLocalEndpoint().getUuid();
        final UUID uuid2 = client2.getLocalEndpoint().getUuid();
        for (Client connectedClient : connectedClients) {
            final UUID uuid = connectedClient.getUuid();
            assertTrue(uuid.equals(uuid1) || uuid.equals(uuid2));
        }

    }

    @Test(timeout = 120000)
    public void testClientListener() {
        final HecatonInstance instance = hecatonFactory.newHecatonInstance();
        final ClientService clientService = instance.getClientService();
        final CountDownLatch latchAdd = new CountDownLatch(2);
        final CountDownLatch latchRemove = new CountDownLatch(2);
        final AtomicInteger totalAdd = new AtomicInteger();

        final ClientListener clientListener = new ClientListener() {
            @Override
            public void clientConnected(Client client) {
                totalAdd.incrementAndGet();
                latchAdd.countDown();
            }

            @Override
            public void clientDisconnected(Client client) {
                latchRemove.countDown();
            }
        };
        final UUID id = clientService.addClientListener(clientListener);

        final HecatonInstance client1 = hecatonFactory.newHecatonClient();
        final HecatonInstance client2 = hecatonFactory.newHecatonClient();

        client1.getLifecycleService().shutdown();
        client2.getLifecycleService().shutdown();

        assertOpenEventually(latchAdd);
        assertOpenEventually(latchRemove);

        assertTrue(clientService.removeClientListener(id));

        assertFalse(clientService.removeClientListener(UUID.randomUUID()));

        assertEquals(0, clientService.getConnectedClients().size());

        hecatonFactory.newHecatonClient();

        assertTrueEventually(() -> assertEquals(1, clientService.getConnectedClients().size()));

        assertEquals(2, totalAdd.get());
    }

    @Test(timeout = 120000)
    public void testConnectedClientsWithReAuth() {
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE)
                .setMultiplier(1).setInitialBackoffMillis(5000);


        HecatonInstance instance = hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        ReconnectListener reconnectListener = new ReconnectListener();
        client.getLifecycleService().addLifecycleListener(reconnectListener);
        //restart the node
        instance.shutdown();
        final HecatonInstance restartedInstance = hecatonFactory.newHecatonInstance();

        client.getMap(randomMapName()).size(); // do any operation

        assertOpenEventually(reconnectListener.reconnectedLatch); //wait for clients to reconnect & reAuth
        assertTrueEventually(() -> assertEquals(1, restartedInstance.getClientService().getConnectedClients().size()));
    }

    @Test(timeout = 120000)
    public void testClientListenerForBothNodes() {
        final HecatonInstance instance1 = hecatonFactory.newHecatonInstance();
        final HecatonInstance instance2 = hecatonFactory.newHecatonInstance();
        final ClientConnectedListenerLatch clientListenerLatch = new ClientConnectedListenerLatch(2);

        final ClientService clientService1 = instance1.getClientService();
        clientService1.addClientListener(clientListenerLatch);
        final ClientService clientService2 = instance2.getClientService();
        clientService2.addClientListener(clientListenerLatch);

        final HecatonInstance client = hecatonFactory.newHecatonClient();
        final String instance1Key = generateKeyOwnedBy(instance1);
        final String instance2Key = generateKeyOwnedBy(instance2);

        final IMap<Object, Object> map = client.getMap("map");
        map.put(instance1Key, 0);
        map.put(instance2Key, 0);

        assertClientConnected(clientService1, clientService2);
        assertOpenEventually(clientListenerLatch);
    }

    @Test
    public void testClientListenerDisconnected() {
        Config config = new Config();
        config.setProperty(ClusterProperty.IO_THREAD_COUNT.getName(), "1");

        final HecatonInstance hz = hecatonFactory.newHecatonInstance(config);
        final HecatonInstance hz2 = hecatonFactory.newHecatonInstance(config);

        int clientCount = 10;
        ClientDisconnectedListenerLatch listenerLatch = new ClientDisconnectedListenerLatch(2 * clientCount);
        hz.getClientService().addClientListener(listenerLatch);
        hz2.getClientService().addClientListener(listenerLatch);

        Collection<HecatonInstance> clients = new LinkedList<>();
        for (int i = 0; i < clientCount; i++) {
            HecatonInstance client = hecatonFactory.newHecatonClient();
            IMap<Object, Object> map = client.getMap(randomMapName());

            map.addEntryListener(new EntryAdapter<>(), true);
            map.put(generateKeyOwnedBy(hz), "value");
            map.put(generateKeyOwnedBy(hz2), "value");

            clients.add(client);
        }

        ExecutorService ex = Executors.newFixedThreadPool(4);
        try {
            for (final HecatonInstance client : clients) {
                ex.execute(client::shutdown);
            }

            assertOpenEventually("Not all disconnected events arrived", listenerLatch);

            assertTrueEventually("First server still have connected clients", () -> assertEquals(0, hz.getClientService().getConnectedClients().size()));
            assertTrueEventually("Second server still have connected clients", () -> assertEquals(0, hz2.getClientService().getConnectedClients().size()));
        } finally {
            ex.shutdown();
        }
    }

    private void assertClientConnected(ClientService... services) {
        for (final ClientService service : services) {
            assertTrueEventually(() -> assertEquals(1, service.getConnectedClients().size()));
        }
    }

    static class ClientConnectedListenerLatch extends CountDownLatch implements ClientListener {

        ClientConnectedListenerLatch(int count) {
            super(count);
        }

        @Override
        public void clientConnected(Client client) {
            countDown();
        }

        @Override
        public void clientDisconnected(Client client) {
        }
    }

    static class ClientDisconnectedListenerLatch extends CountDownLatch implements ClientListener {

        ClientDisconnectedListenerLatch(int count) {
            super(count);
        }

        @Override
        public void clientConnected(Client client) {
        }

        @Override
        public void clientDisconnected(Client client) {
            countDown();
        }
    }


    @Test
    public void testClientShutdownDuration_whenServersDown() {
        HecatonInstance hecatonInstance = hecatonFactory.newHecatonInstance();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        ClientConfig config = new ClientConfig();
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(2000);
        HecatonInstance client = hecatonFactory.newHecatonClient(config);
        client.getLifecycleService().addLifecycleListener(event -> {
            if (event.getState() == LifecycleEvent.LifecycleState.SHUTDOWN) {
                countDownLatch.countDown();
            }
        });

        hecatonInstance.shutdown();
        assertOpenEventually(countDownLatch, ClientExecutionServiceImpl.TERMINATE_TIMEOUT_SECONDS);
    }

    @Test
    public void testClientListener_fromConfig() {
        Config config = new Config();
        final CountDownLatch connectedLatch = new CountDownLatch(1);
        final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        ListenerConfig listenerConfig = new ListenerConfig(new ClientListener() {
            @Override
            public void clientConnected(Client client) {
                connectedLatch.countDown();
            }

            @Override
            public void clientDisconnected(Client client) {
                disconnectedLatch.countDown();
            }
        });

        config.addListenerConfig(listenerConfig);

        HecatonInstance instance = hecatonFactory.newHecatonInstance(config);
        HecatonInstance client = hecatonFactory.newHecatonClient();
        client.shutdown();
        assertOpenEventually(connectedLatch);
        assertOpenEventually(disconnectedLatch);
        instance.shutdown();
    }

    @Test
    public void testClientListener_withSingleMemberClient() {
        Config config = new Config();
        final CountDownLatch latch = new CountDownLatch(2);
        final AtomicInteger eventCount = new AtomicInteger();
        ListenerConfig listenerConfig = new ListenerConfig(new ClientListener() {
            @Override
            public void clientConnected(Client client) {
                eventCount.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void clientDisconnected(Client client) {
                eventCount.incrementAndGet();
                latch.countDown();
            }
        });

        config.addListenerConfig(listenerConfig);

        hecatonFactory.newHecatonInstance(config);
        hecatonFactory.newHecatonInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        client.shutdown();
        assertOpenEventually(latch);
        //client events will only be fired from one of the nodes.
        assertTrueAllTheTime(() -> assertEquals(2, eventCount.get()), 4);
    }

    @Test
    public void testClientListener_withShuttingDownOwnerMember() {
        Config config = new Config();
        final AtomicInteger atomicInteger = new AtomicInteger();
        ListenerConfig listenerConfig = new ListenerConfig(new ClientListener() {
            @Override
            public void clientConnected(Client client) {
                atomicInteger.incrementAndGet();
            }

            @Override
            public void clientDisconnected(Client client) {
                atomicInteger.incrementAndGet();
            }
        });

        config.addListenerConfig(listenerConfig);
        HecatonInstance instance = hecatonFactory.newHecatonInstance();
        //first member is owner connection
        HecatonInstance client = hecatonFactory.newHecatonClient();

        config.setProperty(ClusterProperty.CLIENT_CLEANUP_TIMEOUT.getName(), String.valueOf(Integer.MAX_VALUE));
        hecatonFactory.newHecatonInstance(config);
        //make sure connected to second one before proceeding
        makeSureConnectedToServers(client, 2);

        //when first node is dead, client selects second one as owner
        instance.shutdown();
        //Testing that shutting down first member does not cause any client connected/disconnected event
        assertTrueAllTheTime(() -> assertEquals(1, atomicInteger.get()), 4);
    }

}
