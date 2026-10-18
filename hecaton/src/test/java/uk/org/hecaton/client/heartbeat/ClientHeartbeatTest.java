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

package uk.org.hecaton.client.heartbeat;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.connection.ClientConnectionManager;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.ClientAddPartitionLostListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.ClientRemovePartitionLostListenerCodec;
import uk.org.hecaton.client.impl.spi.ClientListenerService;
import uk.org.hecaton.client.impl.spi.EventHandler;
import uk.org.hecaton.client.impl.spi.impl.ListenerMessageCodec;
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.client.util.ConfigRoutingUtil;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.cluster.MembershipEvent;
import uk.org.hecaton.cluster.MembershipListener;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.LifecycleEvent;
import uk.org.hecaton.core.LifecycleListener;
import uk.org.hecaton.core.LifecycleService;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.nio.ConnectionListener;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spi.exception.TargetDisconnectedException;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static uk.org.hecaton.test.SplitBrainTestSupport.blockCommunicationBetween;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientHeartbeatTest extends ClientTestSupport {

    @Parameterized.Parameter
    public RoutingMode routingMode;

    @Parameterized.Parameters(name = "{index}: routingMode={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(RoutingMode.SINGLE_MEMBER, RoutingMode.ALL_MEMBERS);
    }

    private static final int HEARTBEAT_TIMEOUT_MILLIS = 10000;

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @After
    public void cleanup() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testConnectionClosed_whenHeartbeatStopped() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        HecatonInstance server1 = hecatonFactory.newHecatonInstance(config);
        final HecatonInstance client = hecatonFactory.newHecatonClient(newClientConfig());
        HecatonInstance server2 = hecatonFactory.newHecatonInstance(config);

        HecatonClientInstanceImpl clientImpl = getHecatonClientInstanceImpl(client);
        final ClientConnectionManager connectionManager = clientImpl.getConnectionManager();

        int expectedServerCount = routingMode == RoutingMode.ALL_MEMBERS ? 2 : 1;
        makeSureConnectedToServers(client, expectedServerCount);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        connectionManager.addConnectionListener(new ConnectionListener() {
            @Override
            public void connectionAdded(Connection connection) {

            }

            @Override
            public void connectionRemoved(Connection connection) {
                countDownLatch.countDown();
            }
        });

        if (routingMode == RoutingMode.ALL_MEMBERS) {
            blockMessagesFromInstance(server2, client);
        } else {
            blockMessagesFromInstance(server1, client);
        }
        assertOpenEventually(countDownLatch);
    }

    @Test
    public void testInvocation_whenHeartbeatStopped() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        HecatonInstance server = hecatonFactory.newHecatonInstance(config);
        HecatonInstance client = hecatonFactory.newHecatonClient(newClientConfig());

        warmUpPartitions(server, client);

        IMap<String, String> map = client.getMap("test");
        map.put("foo", "bar");

        blockMessagesFromInstance(server, client);

        expectedException.expect(TargetDisconnectedException.class);
        map.put("for", "bar2");
    }

    @Test
    public void testAsyncInvocation_whenHeartbeatStopped() throws Throwable {
        HecatonInstance server1 = hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient(newClientConfig());
        HecatonInstance server2 = hecatonFactory.newHecatonInstance();

        // make sure client is connected to server2
        IMap<String, String> map = client.getMap(randomString());
        String keyOwnedByServer2 = generateKeyOwnedBy(server2);

        // Verify that the client received partition update for server2
        waitClientPartitionUpdateForKeyOwner(client, server2, keyOwnedByServer2);

        // Make sure that client connects to server2
        map.put(keyOwnedByServer2, randomString());

        // double check that the connection to servers is alive
        int expectedServerCount = routingMode == RoutingMode.ALL_MEMBERS ? 2 : 1;
        makeSureConnectedToServers(client, expectedServerCount);

        if (routingMode == RoutingMode.ALL_MEMBERS) {
            blockMessagesFromInstance(server2, client);
        } else {
            blockMessagesFromInstance(server1, client);
        }

        expectedException.expect(TargetDisconnectedException.class);
        try {
            map.putAsync(keyOwnedByServer2, randomString()).toCompletableFuture().get();
        } catch (ExecutionException e) {
            //unwrap exception
            throw e.getCause();
        }
    }

    @Test
    public void testInvocation_whenHeartbeatResumed() {
        hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient(newClientConfig());
        HecatonInstance instance2 = hecatonFactory.newHecatonInstance();

        // make sure client is connected to instance2
        String keyOwnedByInstance2 = generateKeyOwnedBy(instance2);
        waitClientPartitionUpdateForKeyOwner(client, instance2, keyOwnedByInstance2);

        IMap<String, String> map = client.getMap(randomString());
        map.put(keyOwnedByInstance2, randomString());

        blockMessagesFromInstance(instance2, client);
        sleepMillis(HEARTBEAT_TIMEOUT_MILLIS * 2);
        unblockMessagesFromInstance(instance2, client);

        map.put(keyOwnedByInstance2, randomString());
    }

    private void waitClientPartitionUpdateForKeyOwner(HecatonInstance client, HecatonInstance instance2,
                                                      String keyOwnedByInstance2) {
        // Verify that the client received partition update for instance2
        assertTrueEventually(() -> assertEquals(instance2.getCluster().getLocalMember(),
                client.getPartitionService().getPartition(keyOwnedByInstance2).getOwner()));
    }

    private ClientConfig newClientConfig() {
        ClientConfig clientConfig = ConfigRoutingUtil.newClientConfig(routingMode);
        clientConfig.setProperty(ClientProperty.HEARTBEAT_TIMEOUT.getName(), String.valueOf(HEARTBEAT_TIMEOUT_MILLIS));
        clientConfig.setProperty(ClientProperty.HEARTBEAT_INTERVAL.getName(), "500");
        clientConfig.getConnectionStrategyConfig()
                .getConnectionRetryConfig().setClusterConnectTimeoutMillis(5000);

        return clientConfig;
    }

    @Test
    public void testClientEndpointsDelaySeconds_whenHeartbeatResumed() {
        int delaySeconds = 2;
        Config config = new Config();
        config.setProperty(ClusterProperty.CLIENT_CLEANUP_TIMEOUT.getName(), String.valueOf(TimeUnit.SECONDS.toMillis(delaySeconds)));
        HecatonInstance hecatonInstance = hecatonFactory.newHecatonInstance(config);

        HecatonInstance client = hecatonFactory.newHecatonClient(newClientConfig());

        final CountDownLatch disconnectedLatch = new CountDownLatch(1);

        LifecycleService lifecycleService = client.getLifecycleService();
        lifecycleService.addLifecycleListener(event -> {
            if (LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED == event.getState()) {
                disconnectedLatch.countDown();
            }
        });

        blockMessagesFromInstance(hecatonInstance, client);
        //Wait for client to disconnect because of hearBeat problem.
        assertOpenEventually(disconnectedLatch);

        final CountDownLatch connectedLatch = new CountDownLatch(1);
        final AtomicLong stateChangeCount = new AtomicLong();
        lifecycleService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                stateChangeCount.incrementAndGet();
                Logger.getLogger(this.getClass()).info("state event: " + event);
                if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED == event.getState()) {
                    connectedLatch.countDown();
                }
            }
        });

        unblockMessagesFromInstance(hecatonInstance, client);
        //Wait for client to connect back after heartbeat issue is resolved
        assertOpenEventually(connectedLatch);

        //After client connected, there should not be further change in client state
        //We are specifically testing for scheduled ClientDisconnectionOperation not to take action when run
        assertTrueAllTheTime(() -> assertEquals(1, stateChangeCount.get()), delaySeconds * 2);
    }

    @Test
    public void testAddingListenerToNewConnectionFailedBecauseOfHeartbeat() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        HecatonInstance server1 = hecatonFactory.newHecatonInstance(config);

        final HecatonInstance client = hecatonFactory.newHecatonClient(newClientConfig());

        HecatonClientInstanceImpl clientInstanceImpl = getHecatonClientInstanceImpl(client);
        final ClientListenerService clientListenerService = clientInstanceImpl.getListenerService();
        final CountDownLatch blockIncoming = new CountDownLatch(1);
        final CountDownLatch heartbeatStopped = new CountDownLatch(1);
        final CountDownLatch onListenerRegister = new CountDownLatch(2);
        clientInstanceImpl.getConnectionManager().addConnectionListener(new ConnectionListener() {
            @Override
            public void connectionAdded(Connection connection) {

            }

            @Override
            public void connectionRemoved(Connection connection) {
                heartbeatStopped.countDown();
            }

        });

        clientListenerService.registerListener(createPartitionLostListenerCodec(), new EventHandler() {
            final AtomicInteger count = new AtomicInteger();

            @Override
            public void handle(Object event) {

            }

            @Override
            public void beforeListenerRegister(Connection connection) {
                if (count.incrementAndGet() == 2) {
                    try {
                        blockIncoming.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            @Override
            public void onListenerRegister(Connection connection) {
                onListenerRegister.countDown();
            }

        });

        HecatonInstance server2 = hecatonFactory.newHecatonInstance(config);

        assertTrueEventually(()
                -> {
            int expectedActive = routingMode == RoutingMode.ALL_MEMBERS ? 2 : 1;
            assertEquals(expectedActive,
                    clientInstanceImpl.getConnectionManager().getActiveConnections().size());
        });

        if (routingMode == RoutingMode.ALL_MEMBERS) {
            blockMessagesFromInstance(server2, client);
        } else {
            blockMessagesFromInstance(server1, client);
        }
        assertOpenEventually(heartbeatStopped);
        blockIncoming.countDown();

        if (routingMode == RoutingMode.ALL_MEMBERS) {
            unblockMessagesFromInstance(server2, client);
        } else {
            unblockMessagesFromInstance(server1, client);
        }
        assertOpenEventually(onListenerRegister);
    }

    private ListenerMessageCodec createPartitionLostListenerCodec() {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return ClientAddPartitionLostListenerCodec.encodeRequest(localOnly);
            }

            @Override
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return ClientAddPartitionLostListenerCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return ClientRemovePartitionLostListenerCodec.encodeRequest(realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return ClientRemovePartitionLostListenerCodec.decodeResponse(clientMessage);
            }
        };
    }

    @Test
    public void testClientMembershipEvents_onSplitBrain() {
        Config config = new Config();
        HecatonInstance instanceA = hecatonFactory.newHecatonInstance(config);

        ClientConfig clientConfig = newClientConfig();
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig()
                .setClusterConnectTimeoutMillis(Integer.MAX_VALUE);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        HecatonInstance instanceB = hecatonFactory.newHecatonInstance(config);
        HecatonInstance instanceC = hecatonFactory.newHecatonInstance(config);

        Member memberA = instanceA.getCluster().getLocalMember();
        Member memberB = instanceB.getCluster().getLocalMember();
        Member memberC = instanceC.getCluster().getLocalMember();

        CountDownLatch splitLatch = new CountDownLatch(1);
        CountDownLatch bRemovedLatch = new CountDownLatch(1);
        CountDownLatch aRemovedLatch = new CountDownLatch(1);

        CountDownLatch switchedToCLatch = new CountDownLatch(1);
        client.getCluster().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                if (memberC.equals(membershipEvent.getMember())) {
                    switchedToCLatch.countDown();
                }
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                if (memberC.equals(membershipEvent.getMember())) {
                    splitLatch.countDown();
                }

                if (memberB.equals(membershipEvent.getMember())) {
                    bRemovedLatch.countDown();
                }

                if (memberA.equals(membershipEvent.getMember())) {
                    aRemovedLatch.countDown();
                }
            }
        });

        blockCommunicationBetween(instanceA, instanceC);
        closeConnectionBetween(instanceA, instanceC);
        blockCommunicationBetween(instanceB, instanceC);
        closeConnectionBetween(instanceB, instanceC);

        assertOpenEventually(" A B | C", splitLatch);
        instanceB.shutdown();
        assertOpenEventually(" A | C", bRemovedLatch);
        instanceA.shutdown();
        assertOpenEventually("_ | C", aRemovedLatch);

        assertOpenEventually("Client should connect to C and see C as memberAdded", switchedToCLatch);
    }
}
