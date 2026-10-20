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

package uk.org.hecaton.internal.networking.nio;

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.internal.nio.Protocols;
import uk.org.hecaton.internal.server.ServerConnection;
import uk.org.hecaton.internal.server.tcp.ServerSocketRegistry;
import uk.org.hecaton.internal.server.tcp.TcpServer;
import uk.org.hecaton.internal.server.tcp.TcpServerConnection;
import uk.org.hecaton.jet.datamodel.Tuple2;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.util.Objects;
import java.util.Set;

import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static uk.org.hecaton.test.HecatonTestSupport.assertClusterSizeEventually;
import static uk.org.hecaton.test.HecatonTestSupport.smallInstanceConfig;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HecatonSerialClassRunner.class)
public class AdvancedNetworkIntegrationTest extends AbstractAdvancedNetworkIntegrationTest {

    int firstMemberPort = 6000;
    int firstClientPort = 7000;
    int secondMemberPort = 8000;

    @Test
    @Category(QuickTest.class)
    public void testCompleteMultisocketConfig() {
        Config config = createCompleteMultiSocketConfig();
        newHecatonInstance(config);
        assertLocalPortsOpen(MEMBER_PORT, CLIENT_PORT, WAN1_PORT, WAN2_PORT, REST_PORT, MEMCACHE_PORT);

        // Test if invalid protocol given then instance replies with HZX(UNEXPECTED_PROTOCOL)
        assertWrongProtocolAlert(MEMBER_PORT, Protocols.CLIENT_BINARY, Protocols.HAZELCAST_CLUSTER, "AAA");
        assertWrongProtocolAlert(CLIENT_PORT, Protocols.CLUSTER, "AAA");
        assertWrongProtocolAlert(WAN1_PORT, Protocols.CLIENT_BINARY, "AAA");
        assertWrongProtocolAlert(WAN2_PORT, Protocols.CLIENT_BINARY, "AAA");

        assertWrongProtocolAlert(REST_PORT, Protocols.CLIENT_BINARY, "AAA");
        assertWrongProtocolAlert(REST_PORT, Protocols.CLUSTER, "AAA");
        assertWrongProtocolAlert(MEMCACHE_PORT, Protocols.CLIENT_BINARY, "AAA");
        assertWrongProtocolAlert(MEMCACHE_PORT, Protocols.CLUSTER, "AAA");
    }

    @Test
    @Category(SlowTest.class)
    public void testMembersReportAllAddresses() {
        Config config = createCompleteMultiSocketConfig();
        for (int i = 0; i < 3; i++) {
            newHecatonInstance(config);
        }
        assertClusterSizeEventually(3, instances);

        for (HecatonInstance hz : instances) {
            Set<Member> members = hz.getCluster().getMembers();
            for (Member member : members) {
                assertEquals(6, member.getAddressMap().size());
            }
        }
    }

    @Test
    @Category(QuickTest.class)
    public void testKeepAliveSocketOptions() throws Throwable {
        assumeKeepAlivePerSocketOptionsSupported();
        Config config = createCompleteMultiSocketConfig();
        config.getAdvancedNetworkConfig().getEndpointConfigs().get(EndpointQualifier.MEMBER)
                .setSocketKeepAlive(true)
                .setSocketKeepIdleSeconds(5)
                .setSocketKeepCount(2)
                .setSocketKeepIntervalSeconds(1);

        HecatonInstance hz = newHecatonInstance(config);
        newHecatonInstance(config);

        assertClusterSizeEventually(2, hz);

        TcpServer tcpServer = (TcpServer) Accessors.getNode(hz).getServer();
        ServerSocketRegistry registry = tcpServer.getRegistry();
        for (ServerSocketRegistry.Pair pair : registry) {
            if (EndpointQualifier.MEMBER.equals(pair.getQualifier())) {
                assertEquals(2, (int) pair.getChannel().getOption(IOUtil.JDK_NET_TCP_KEEPCOUNT));
                assertEquals(1, (int) pair.getChannel().getOption(IOUtil.JDK_NET_TCP_KEEPINTERVAL));
                assertEquals(5, (int) pair.getChannel().getOption(IOUtil.JDK_NET_TCP_KEEPIDLE));
            }
        }

        for (ServerConnection c : tcpServer.getConnectionManager(EndpointQualifier.MEMBER).getConnections()) {
            TcpServerConnection cxn = (TcpServerConnection) c;
            AbstractChannel ch = (AbstractChannel) cxn.getChannel();
            assertEquals(2, (int) ch.socketChannel().getOption(IOUtil.JDK_NET_TCP_KEEPCOUNT));
            assertEquals(1, (int) ch.socketChannel().getOption(IOUtil.JDK_NET_TCP_KEEPINTERVAL));
            assertEquals(5, (int) ch.socketChannel().getOption(IOUtil.JDK_NET_TCP_KEEPIDLE));
        }
    }

    @Test
    @Category(QuickTest.class)
    public void testKeepAliveSocketOptions_whenNotSupported() throws Throwable {
        assumeKeepAlivePerSocketOptionsNotSupported();
        // ensure that even though options are configured and setting them fails, no exceptions are thrown
        Config config = createCompleteMultiSocketConfig();
        config.getAdvancedNetworkConfig().getEndpointConfigs().get(EndpointQualifier.MEMBER)
                .setSocketKeepAlive(true)
                .setSocketKeepIdleSeconds(5)
                .setSocketKeepCount(2)
                .setSocketKeepIntervalSeconds(1);

        HecatonInstance hz = newHecatonInstance(config);
        newHecatonInstance(config);

        assertClusterSizeEventually(2, hz);
    }

    @Test(expected = AssertionError.class)
    @Category(QuickTest.class)
    public void testLocalPortAssertionWorks() {
        assertLocalPortsOpen(MEMBER_PORT);
    }

    @Test
    @Category(SlowTest.class)
    public void testConnectionToWrongPort() {
        Tuple2<Config, Config> cfgTuple = prepareConfigs();

        // Mis-configured to point to Client port of 1st member
        Config config = cfgTuple.f1();
        Objects.requireNonNull(config)
                .getAdvancedNetworkConfig()
                .getJoin()
                .getTcpIpConfig()
                .setEnabled(true)
                .addMember("127.0.0.1:" + firstClientPort);

        newHecatonInstance(cfgTuple.f0());

        assertThatThrownBy(() -> newHecatonInstance(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Node failed to start!");
    }

    @Test
    @Category(SlowTest.class)
    public void testConnectionToWrongPortWithRequiredMember() {
        Tuple2<Config, Config> cfgTuple = prepareConfigs();

        // Mis-configured to point to Client port of 1st member
        Config config = cfgTuple.f1();
        Objects.requireNonNull(config)
                .getAdvancedNetworkConfig()
                .getJoin()
                .getTcpIpConfig()
                .setEnabled(true)
                .setRequiredMember("127.0.0.1:" + firstClientPort);

        newHecatonInstance(cfgTuple.f0());

        assertThatThrownBy(() -> newHecatonInstance(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Node failed to start!");
    }

    @Test
    @Category(SlowTest.class)
    public void doNotShutdownIfSomeMembersCanBeConnected() {
        Tuple2<Config, Config> cfgTuple = prepareConfigs();

        // Mis-configured to point to Client port of 1st member
        // However member port also added to the config
        Objects.requireNonNull(cfgTuple.f1())
                .getAdvancedNetworkConfig()
                .getJoin()
                .getTcpIpConfig()
                .setEnabled(true)
                .addMember("127.0.0.1:" + firstClientPort)
                .addMember("127.0.0.1:" + firstMemberPort);

        newHecatonInstance(cfgTuple.f0());
        newHecatonInstance(cfgTuple.f1());
    }

    @Test
    @Category(SlowTest.class)
    public void doShutdownIfSomeMembersCanBeConnectedWithRequiredMember() {
        Tuple2<Config, Config> cfgTuple = prepareConfigs();

        // Mis-configured to point to Client port of 1st member
        // However member port also added to the config
        Config config = cfgTuple.f1();
        Objects.requireNonNull(config)
                .getAdvancedNetworkConfig()
                .getJoin()
                .getTcpIpConfig()
                .setEnabled(true)
                .setRequiredMember("127.0.0.1:" + firstClientPort)
                .addMember("127.0.0.1:" + firstMemberPort);

        newHecatonInstance(cfgTuple.f0());

        assertThatThrownBy(() -> newHecatonInstance(config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Node failed to start!");
    }

    private void assertLocalPortsOpen(int... ports) {
        for (int port : ports) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", port));
            } catch (IOException e) {
                fail("Failed to connect to port " + port + ": " + e.getMessage());
            }
        }
    }

    protected Socket buildSocket(int port) {
        Socket socket;
        try {
            socket = new Socket("127.0.0.1", port);
        } catch (IOException e) {
            throw rethrow(e);
        }
        return socket;
    }

    private void assertWrongProtocolAlert(int port, String... protocolHeadersToTry) {
        byte[] expected = Protocols.UNEXPECTED_PROTOCOL.getBytes();
        for (String header : protocolHeadersToTry) {
            try (Socket socket = buildSocket(port)) {
                socket.getOutputStream().write(header.getBytes());
                byte[] response = new byte[3];
                IOUtil.readFully(socket.getInputStream(), response);
                assertArrayEquals(
                        "The protocol header " + header + " should be unexpected on port " + port,
                        expected, response);
            } catch (IOException e) {
                fail("Failed to connect to port " + port + ": " + e.getMessage());
            }
        }
    }

    private Tuple2<Config, Config> prepareConfigs() {
        Config config = smallInstanceConfig();
        config.getAdvancedNetworkConfig().setEnabled(true);
        config.getAdvancedNetworkConfig().setMemberEndpointConfig(createServerSocketConfig(firstMemberPort))
                .setClientEndpointConfig(createServerSocketConfig(firstClientPort));
        JoinConfig joinConfig = config.getAdvancedNetworkConfig().getJoin();
        joinConfig.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1:" + secondMemberPort);

        Config other = smallInstanceConfig();
        other.getAdvancedNetworkConfig().setEnabled(true);
        other.getAdvancedNetworkConfig().setMemberEndpointConfig(createServerSocketConfig(secondMemberPort));
        other.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "1");

        return Tuple2.tuple2(config, other);
    }

    private void assumeKeepAlivePerSocketOptionsNotSupported() throws Throwable {
        Assume.assumeFalse(socketSupportsKeepAliveOptions());
    }

    private void assumeKeepAlivePerSocketOptionsSupported() throws Throwable {
        Assume.assumeTrue(socketSupportsKeepAliveOptions());
    }

    private boolean socketSupportsKeepAliveOptions() throws Throwable {
        try (ServerSocketChannel serverSocketChannel = buildServerSocket()) {
            return IOUtil.supportsKeepAliveOptions(serverSocketChannel);
        }
    }

    private static ServerSocketChannel buildServerSocket() throws IOException {
        InetSocketAddress socketAddress = new InetSocketAddress("127.0.0.1", 0);
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        ServerSocket server = serverSocketChannel.socket();
        server.bind(socketAddress);
        return serverSocketChannel;
    }
}
