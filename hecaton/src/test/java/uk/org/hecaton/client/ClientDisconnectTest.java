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

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.internal.nio.ConnectionListener;
import uk.org.hecaton.internal.server.ServerConnection;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.operationparker.impl.OperationParkerImpl;
import uk.org.hecaton.spi.impl.operationservice.impl.InvocationRegistry;
import uk.org.hecaton.spi.impl.operationservice.impl.OperationServiceImpl;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.topic.Message;
import uk.org.hecaton.topic.ReliableMessageListener;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientDisconnectTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void cleanup() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testClientOperationCancelled_whenDisconnected() throws Exception {
        Config config = new Config();
        config.setProperty(ClusterProperty.CLIENT_CLEANUP_TIMEOUT.getName(), String.valueOf(Integer.MAX_VALUE));
        HecatonInstance hecatonInstance = hecatonFactory.newHecatonInstance(config);
        final String queueName = "q";

        final HecatonInstance clientInstance = hecatonFactory.newHecatonClient();
        final UUID uuid = clientInstance.getLocalEndpoint().getUuid();
        final CountDownLatch clientDisconnectedFromNode = new CountDownLatch(1);
        hecatonInstance.getClientService().addClientListener(new ClientListener() {
            @Override
            public void clientConnected(Client client) {

            }

            @Override
            public void clientDisconnected(Client client) {
                if (client.getUuid().equals(uuid)) {
                    clientDisconnectedFromNode.countDown();
                }
            }
        });
        new Thread(() -> {
            IQueue<Integer> queue = clientInstance.getQueue(queueName);
            try {
                queue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (HecatonInstanceNotActiveException e) {
                ignore(e);
            }
        }).start();

        SECONDS.sleep(2);

        clientInstance.shutdown();
        assertOpenEventually(clientDisconnectedFromNode);

        final IQueue<Integer> queue = hecatonInstance.getQueue(queueName);
        queue.add(1);
        //dead client should not be able to consume item from queue
        assertTrueAllTheTime(() -> assertEquals(1, queue.size()), 3);
    }

    @Test
    public void testClientOperationCancelled_whenDisconnected_lock() throws Exception {
        Config config = new Config();
        config.setProperty(ClusterProperty.CLIENT_CLEANUP_TIMEOUT.getName(), String.valueOf(Integer.MAX_VALUE));
        HecatonInstance hecatonInstance = hecatonFactory.newHecatonInstance(config);
        final String name = "m";

        final IMap<Object, Object> map = hecatonInstance.getMap(name);
        final String key = "key";
        map.lock(key);

        final HecatonInstance clientInstance = hecatonFactory.newHecatonClient();
        final CountDownLatch clientDisconnectedFromNode = new CountDownLatch(1);
        final UUID uuid = clientInstance.getLocalEndpoint().getUuid();
        hecatonInstance.getClientService().addClientListener(new ClientListener() {
            @Override
            public void clientConnected(Client client) {

            }

            @Override
            public void clientDisconnected(Client client) {
                if (client.getUuid().equals(uuid)) {
                    clientDisconnectedFromNode.countDown();
                }
            }
        });
        new Thread(() -> {
            IMap<Object, Object> clientMap = clientInstance.getMap(name);
            try {
                clientMap.lock(key);
            } catch (Exception e) {
                ignore(e);
            }

        }).start();

        SECONDS.sleep(2);

        clientInstance.shutdown();
        assertOpenEventually(clientDisconnectedFromNode);

        map.unlock(key);
        //dead client should not be able to acquire the lock.
        assertTrueAllTheTime(() -> assertFalse(map.isLocked(key)), 3);
    }

    @Test
    public void testPendingInvocationAndWaitEntryCancelled_whenDisconnected_withQueue() {
        Config config = new Config();
        HecatonInstance server = hecatonFactory.newHecatonInstance(config);
        final String name = randomName();

        final HecatonInstance client = hecatonFactory.newHecatonClient();

        spawn((Runnable) () -> {
            try {
                client.getQueue(name).take();
            } catch (Throwable ignored) {
            }
        });

        assertNonEmptyPendingInvocationAndWaitSet(server);

        client.shutdown();

        assertEmptyPendingInvocationAndWaitSet(server);
    }

    @Test
    public void testPendingInvocationAndWaitEntryCancelled_whenDisconnected_withReliableTopic() {
        Config config = new Config();
        HecatonInstance server = hecatonFactory.newHecatonInstance(config);

        HecatonInstance client = hecatonFactory.newHecatonClient();
        // ReliableTopic listener registers a blocking invocation
        client.getReliableTopic(randomName()).addMessageListener(new NopReliableMessageListener());

        assertNonEmptyPendingInvocationAndWaitSet(server);

        client.shutdown();

        assertEmptyPendingInvocationAndWaitSet(server);
    }

    private void assertNonEmptyPendingInvocationAndWaitSet(HecatonInstance server) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(server);
        OperationServiceImpl operationService = nodeEngine.getOperationService();
        final InvocationRegistry invocationRegistry = operationService.getInvocationRegistry();
        final OperationParkerImpl operationParker = (OperationParkerImpl) nodeEngine.getOperationParker();

        assertTrueEventually(() -> assertFalse(invocationRegistry.entrySet().isEmpty()));

        assertTrueEventually(() -> assertTrue(operationParker.getTotalParkedOperationCount() > 0));
    }

    private void assertEmptyPendingInvocationAndWaitSet(HecatonInstance server) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(server);
        OperationServiceImpl operationService = nodeEngine.getOperationService();
        final InvocationRegistry invocationRegistry = operationService.getInvocationRegistry();
        final OperationParkerImpl operationParker = (OperationParkerImpl) nodeEngine.getOperationParker();

        assertTrueEventually(() -> assertTrue(invocationRegistry.entrySet().isEmpty()));

        assertTrueEventually(() -> assertEquals(0, operationParker.getTotalParkedOperationCount()));
    }

    private static class NopReliableMessageListener implements ReliableMessageListener<Object> {
        @Override
        public long retrieveInitialSequence() {
            return 0;
        }

        @Override
        public void storeSequence(long sequence) {
        }

        @Override
        public boolean isLossTolerant() {
            return false;
        }

        @Override
        public boolean isTerminal(Throwable failure) {
            return false;
        }

        @Override
        public void onMessage(Message<Object> message) {
        }
    }

    @Test
    public void testConnectionEventsFiredForClientsOnServerConnectionManager() {
        HecatonInstance server = hecatonFactory.newHecatonInstance();

        CountDownLatch clientConnected = new CountDownLatch(1);
        CountDownLatch clientDisconnected = new CountDownLatch(1);
        getNodeEngineImpl(server).getNode().getServer().addConnectionListener(new ConnectionListener<>() {
            @Override
            public void connectionAdded(ServerConnection connection) {
                if (connection.isClient()) {
                    clientConnected.countDown();
                }
            }

            @Override
            public void connectionRemoved(ServerConnection connection) {
                if (connection.isClient()) {
                    clientDisconnected.countDown();
                }
            }
        });

        HecatonInstance client = hecatonFactory.newHecatonClient();
        assertOpenEventually(clientConnected);

        client.shutdown();
        assertOpenEventually(clientDisconnected);
    }
}
