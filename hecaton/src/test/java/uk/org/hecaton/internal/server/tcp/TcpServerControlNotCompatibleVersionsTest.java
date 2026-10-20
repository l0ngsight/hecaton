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
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ServerSocketEndpointConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.instance.ProtocolType;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.networking.Channel;
import uk.org.hecaton.internal.nio.ConnectionLifecycleListener;
import uk.org.hecaton.internal.nio.ConnectionType;
import uk.org.hecaton.internal.nio.Packet;
import uk.org.hecaton.nio.serialization.HecatonSerializationException;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.TestAwareInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static uk.org.hecaton.instance.ProtocolType.WAN;
import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.HecatonTestSupport.smallInstanceConfig;
import static uk.org.hecaton.test.starter.ReflectionUtils.getFieldValueReflectively;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category(QuickTest.class)
public class TcpServerControlNotCompatibleVersionsTest {

    // client side socket address of the new connection
    private static final InetSocketAddress CLIENT_SOCKET_ADDRESS = new InetSocketAddress("127.0.0.1", 49152);

    // server-side member addresses
    private static final Address SERVER_MEMBER_ADDRESS;
    private static final Address SERVER_CLIENT_ADDRESS;
    private static final Address SERVER_WAN_ADDRESS;

    static {
        try {
            SERVER_MEMBER_ADDRESS = new Address("127.0.0.1", 5701);
            SERVER_CLIENT_ADDRESS = new Address("127.0.0.1", 6000);
            SERVER_WAN_ADDRESS = new Address("127.0.0.1", 10000);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    // protocol type of endpoint manager that receives bind message
    @Parameter
    public ProtocolType protocolType;

    // connection type of TcpServerConnection for which MemberHandshake is processed
    @Parameter(1)
    public String connectionType;

    // MemberHandshake.reply (true to test MemberHandshake from connection initiator to server,
    // false when the other way around)
    @Parameter(2)
    public boolean reply;

    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    private TcpServerControl tcpServerControl;
    private TcpServerConnection connection;

    // mocks
    private Channel channel;
    private TcpServerConnectionManager connectionManager;

    @Parameters(name = "protocolType: {0}, connectionType: {1}, reply: {2}")
    public static List<Object> parameters() {
        return Arrays.asList(new Object[]{
                // On MEMBER connections, only MEMBER addresses are registered in the acceptor side
                // Initiator client and WAN addresses is also registered in the initiator side
                new Object[]{ProtocolType.MEMBER, ConnectionType.MEMBER, true},
                new Object[]{ProtocolType.MEMBER, ConnectionType.MEMBER, false},
                // when protocol type not supported by BindHandler, nothing is registered
                new Object[]{ProtocolType.CLIENT, null, false},
                new Object[]{WAN, ConnectionType.MEMBER, false},
                new Object[]{WAN, ConnectionType.MEMBER, true}
        });
    }

    @Before
    public void setup() throws IllegalAccessException {
        HecatonInstance hz = factory.newHecatonInstance(createConfig());
        Node node = getNode(hz);
        connectionManager = (TcpServerConnectionManager) node.getServer()
                .getConnectionManager(EndpointQualifier.resolve(protocolType, WAN == protocolType ? "wan" : null));
        tcpServerControl = getFieldValueReflectively(connectionManager, "serverControl");

        // setup mock channel
        channel = mock(Channel.class);
        ConcurrentMap<Object, Object> channelAttributeMap = new ConcurrentHashMap<>();
        when(channel.attributeMap()).thenReturn(channelAttributeMap);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void process() {
        Packet packet = incorrectMemberHandshakeMessage();
        Exception e = assertThrows(HecatonSerializationException.class, () -> tcpServerControl.process(packet));
        assertThat(e).isInstanceOf(HecatonSerializationException.class)
                     .hasMessageContaining("Failed to deserialize member handshake packet received from incompatible version");
        assertThat(connection.isAlive()).isFalse();
    }

    @SuppressWarnings("unchecked")
    private Packet incorrectMemberHandshakeMessage() {
        byte[] bytes = new byte[163];
        Arrays.fill(bytes, (byte) 1);

        Packet packet = new Packet(bytes);
        packet.resetFlagsTo(packet.getFlags() & ~Packet.FLAG_4_0);
        boolean acceptorSide = reply;
        connection = new TcpServerConnection(connectionManager, mock(ConnectionLifecycleListener.class), 1, channel, acceptorSide);
        if (connectionType != null) {
            connection.setConnectionType(connectionType);
        }
        packet.setConn(connection);
        return packet;
    }

    private Config createConfig() {
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
        Config config = smallInstanceConfig();
        config.getAdvancedNetworkConfig()
                .setEnabled(true)
                .setMemberEndpointConfig(memberServerSocketConfig)
                .setClientEndpointConfig(clientServerSocketConfig)
                .addWanEndpointConfig(wanServerSocketConfig);
        return config;
    }
}
