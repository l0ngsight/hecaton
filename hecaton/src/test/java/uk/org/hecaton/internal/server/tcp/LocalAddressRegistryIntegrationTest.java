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

package uk.org.hecaton.internal.server.tcp;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.AdvancedNetworkConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.ServerSocketEndpointConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.server.ServerConnection;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.partition.Partition;
import uk.org.hecaton.partition.PartitionService;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static uk.org.hecaton.spi.properties.ClusterProperty.CHANNEL_COUNT;
import static uk.org.hecaton.test.Accessors.getNode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HecatonSerialClassRunner.class)
@Category(NightlyTest.class)
public class LocalAddressRegistryIntegrationTest extends HecatonTestSupport {

    private static final int CHANNELS_PER_CONNECTION = 3;

    // MEMBER & WAN & CLIENT addresses of the connection initiator
    private static final Address INITIATOR_MEMBER_ADDRESS;
    private static final Address INITIATOR_WAN_ADDRESS;
    private static final Address INITIATOR_CLIENT_ADDRESS;

    // server-side member addresses
    private static final Address SERVER_MEMBER_ADDRESS;
    private static final Address SERVER_CLIENT_ADDRESS;
    private static final Address SERVER_WAN_ADDRESS;

    static {
        try {
            INITIATOR_MEMBER_ADDRESS = new Address("127.0.0.1", 5702);
            INITIATOR_CLIENT_ADDRESS = new Address("127.0.0.1", 6001);
            INITIATOR_WAN_ADDRESS = new Address("127.0.0.1", 9000);

            SERVER_MEMBER_ADDRESS = new Address("127.0.0.1", 5701);
            SERVER_CLIENT_ADDRESS = new Address("127.0.0.1", 6000);
            SERVER_WAN_ADDRESS = new Address("127.0.0.1", 10000);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @After
    public void tearDown() {
        Hecaton.shutdownAll();
    }

    @Test
    public void whenSomeConnectionClosedBetweenMembers_registeredAddresses_should_not_be_cleaned_up() {
        int timeoutSecs = 10;

        // create members
        HecatonInstance serverMember = Hecaton.newHecatonInstance(createConfigForServer());
        HecatonInstance initiatorMember = Hecaton.newHecatonInstance(createConfigForInitiator());
        UUID initiatorMemberUuid = initiatorMember.getCluster().getLocalMember().getUuid();
        assertClusterSize(2, serverMember, initiatorMember);

        createConnectionsOnEachPlane(serverMember);
        Node serverNode = getNode(serverMember);
        TcpServerConnectionManager connectionManager = (TcpServerConnectionManager) serverNode.getServer()
                .getConnectionManager(EndpointQualifier.MEMBER);
        assertTrueEventually(() ->
                assertGreaterOrEquals(
                        "The number of connections must be greater than the number of channels.",
                        connectionManager.getConnections().size(),
                        CHANNELS_PER_CONNECTION
                ), timeoutSecs);

        LinkedAddresses registeredAddressesOfInitiatorMember = serverNode
                .getLocalAddressRegistry()
                .linkedAddressesOf(initiatorMemberUuid);
        assertNotNull(registeredAddressesOfInitiatorMember);
        assertContains(registeredAddressesOfInitiatorMember.getAllAddresses(), INITIATOR_MEMBER_ADDRESS);
        ServerConnection removedConnection = closeRandomConnection(new ArrayList<>(connectionManager.getConnections()));
        assertTrueEventually(() -> assertThat(connectionManager.getConnections()).doesNotContain(removedConnection),
                timeoutSecs);

        LinkedAddresses registeredAddressesAfterConnectionClose = serverNode
                .getLocalAddressRegistry()
                .linkedAddressesOf(initiatorMemberUuid);
        assertNotNull(registeredAddressesAfterConnectionClose);
        assertContains(registeredAddressesAfterConnectionClose.getAllAddresses(), INITIATOR_MEMBER_ADDRESS);

        waitAllForSafeState(serverMember, initiatorMember);
    }

    @Test
    public void whenAllConnectionClosedBetweenMembers_registeredAddresses_should_be_cleaned_up() {
        int timeoutSecs = 10;

        // create members
        HecatonInstance serverMember = Hecaton.newHecatonInstance(createConfigForServer());
        HecatonInstance initiatorMember = Hecaton.newHecatonInstance(createConfigForInitiator());
        UUID iniatiatorMemberUuid = initiatorMember.getCluster().getLocalMember().getUuid();
        assertClusterSize(2, serverMember, initiatorMember);
        createConnectionsOnEachPlane(serverMember);

        Node serverNode = getNode(serverMember);
        TcpServerConnectionManager connectionManager = (TcpServerConnectionManager) serverNode.getServer()
                .getConnectionManager(EndpointQualifier.MEMBER);
        assertTrueEventually(() ->
                assertGreaterOrEquals(
                        "The number of connections must be greater than the number of channels.",
                        connectionManager.getConnections().size(),
                        CHANNELS_PER_CONNECTION
                ), timeoutSecs);

        LinkedAddresses registeredAddressesOfInitiatorMember = serverNode
                .getLocalAddressRegistry()
                .linkedAddressesOf(iniatiatorMemberUuid);

        assertNotNull(registeredAddressesOfInitiatorMember);
        assertContains(registeredAddressesOfInitiatorMember.getAllAddresses(), INITIATOR_MEMBER_ADDRESS);
        initiatorMember.shutdown();
        assertTrueEventually(() ->
                assertEquals(
                        0,
                        connectionManager.getConnections().size()
                ), timeoutSecs);
        LinkedAddresses registeredAddressesAfterAllConnectionsClosed = serverNode
                .getLocalAddressRegistry()
                .linkedAddressesOf(iniatiatorMemberUuid);
        assertNull(registeredAddressesAfterAllConnectionsClosed);

    }


    private Config createConfigForServer() {
        Config config = smallInstanceConfig().setProperty(CHANNEL_COUNT.getName(), String.valueOf(CHANNELS_PER_CONNECTION));
        AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
        JoinConfig advancedJoinConfig = advancedNetworkConfig.getJoin();
        advancedJoinConfig.getTcpIpConfig().setEnabled(true);
        ServerSocketEndpointConfig memberServerSocketConfig = new ServerSocketEndpointConfig()
                .setPort(SERVER_MEMBER_ADDRESS.getPort());
        memberServerSocketConfig.getInterfaces().addInterface(SERVER_MEMBER_ADDRESS.getHost());
        ServerSocketEndpointConfig clientServerSocketConfig = new ServerSocketEndpointConfig()
                .setPort(SERVER_CLIENT_ADDRESS.getPort());
        clientServerSocketConfig.getInterfaces().addInterface(SERVER_CLIENT_ADDRESS.getHost());
        ServerSocketEndpointConfig wanServerSocketConfig = new ServerSocketEndpointConfig()
                .setName("wan")
                .setPort(SERVER_WAN_ADDRESS.getPort());
        wanServerSocketConfig.getInterfaces().addInterface(SERVER_WAN_ADDRESS.getHost());

        memberServerSocketConfig.getInterfaces().addInterface("127.0.0.1");
        advancedNetworkConfig.setEnabled(true)
                .setMemberEndpointConfig(memberServerSocketConfig)
                .setClientEndpointConfig(clientServerSocketConfig)
                .addWanEndpointConfig(wanServerSocketConfig);
        return config;
    }

    private Config createConfigForInitiator() {
        Config config = smallInstanceConfig().setProperty(CHANNEL_COUNT.getName(), String.valueOf(CHANNELS_PER_CONNECTION));
        AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
        JoinConfig advancedJoinConfig = advancedNetworkConfig.getJoin();
        advancedJoinConfig.getTcpIpConfig().setEnabled(true).addMember(
                SERVER_MEMBER_ADDRESS.getHost() + ":" + SERVER_MEMBER_ADDRESS.getPort()
        );

        ServerSocketEndpointConfig memberServerSocketConfig = new ServerSocketEndpointConfig()
                .setPort(INITIATOR_MEMBER_ADDRESS.getPort());
        memberServerSocketConfig.getInterfaces().addInterface(INITIATOR_MEMBER_ADDRESS.getHost());
        ServerSocketEndpointConfig clientServerSocketConfig = new ServerSocketEndpointConfig()
                .setPort(INITIATOR_CLIENT_ADDRESS.getPort());
        ServerSocketEndpointConfig wanServerSocketConfig = new ServerSocketEndpointConfig()
                .setName("wan")
                .setPort(SERVER_WAN_ADDRESS.getPort());
        wanServerSocketConfig.getInterfaces().addInterface(INITIATOR_WAN_ADDRESS.getHost());

        memberServerSocketConfig.getInterfaces().addInterface("127.0.0.1");
        advancedNetworkConfig.setEnabled(true)
                .setMemberEndpointConfig(memberServerSocketConfig)
                .setClientEndpointConfig(clientServerSocketConfig)
                .addWanEndpointConfig(wanServerSocketConfig);

        return config;
    }

    private void createConnectionsOnEachPlane(HecatonInstance hz) {
        IMap<String, String> dummy = hz.getMap(randomMapName());
        hz.getPartitionService().getPartitions().forEach(
                partition -> {
                    String key = randomKeyNameOwnedByPartition(hz, partition.getPartitionId());
                    dummy.put(key, key);
                }
        );
        dummy.destroy();
    }

    private ServerConnection closeRandomConnection(List<ServerConnection> connections) {
        Random random = new Random();
        int randomIdx = random.nextInt(connections.size());
        ServerConnection connection = connections.get(randomIdx);
        connection.close("Failure is injected", null);
        return connection;
    }

    private String randomKeyNameOwnedByPartition(HecatonInstance hz, int partitionId) {
        PartitionService partitionService = hz.getPartitionService();
        while (true) {
            String name = randomString();
            Partition partition = partitionService.getPartition(name);
            if (partition.getPartitionId() == partitionId) {
                return name;
            }
        }
    }
}
