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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.internal.server.ServerConnection;
import uk.org.hecaton.internal.server.tcp.ServerSocketRegistry;
import uk.org.hecaton.internal.server.tcp.TcpServer;
import uk.org.hecaton.internal.server.tcp.TcpServerConnection;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.core.Hecaton.newHecatonInstance;
import static uk.org.hecaton.internal.networking.nio.NetworkTestUtil.assumeKeepAlivePerSocketOptionsNotSupported;
import static uk.org.hecaton.internal.networking.nio.NetworkTestUtil.assumeKeepAlivePerSocketOptionsSupported;
import static uk.org.hecaton.test.HecatonTestSupport.assertClusterSizeEventually;
import static uk.org.hecaton.test.HecatonTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class UnifiedNetworkIntegrationTest {
    @After
    public void tearDown() {
        Hecaton.shutdownAll();
    }

    @Test
    public void testInvalidKeepIdleCount_failsStartup() throws Throwable {
        assumeKeepAlivePerSocketOptionsSupported();
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.SOCKET_KEEP_COUNT.getName(), "-1");
        assertThrows(IllegalArgumentException.class, () -> newHecatonInstance(config));
        config.setProperty(ClusterProperty.SOCKET_KEEP_COUNT.getName(), "128");
        assertThrows(IllegalArgumentException.class, () -> newHecatonInstance(config));
        config.setProperty(ClusterProperty.SOCKET_KEEP_COUNT.getName(), "0");
        assertThrows(IllegalArgumentException.class, () -> newHecatonInstance(config));
    }

    @Test
    public void testInvalidKeepIntervalSeconds_failsStartup() throws Throwable {
        assumeKeepAlivePerSocketOptionsSupported();
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.SOCKET_KEEP_INTERVAL.getName(), "-1");
        assertThrows(IllegalArgumentException.class, () -> newHecatonInstance(config));
        config.setProperty(ClusterProperty.SOCKET_KEEP_INTERVAL.getName(), "32768");
        assertThrows(IllegalArgumentException.class, () -> newHecatonInstance(config));
        config.setProperty(ClusterProperty.SOCKET_KEEP_INTERVAL.getName(), "0");
        assertThrows(IllegalArgumentException.class, () -> newHecatonInstance(config));
    }

    @Test
    public void testInvalidKeepIdleSeconds_failsStartup() throws Throwable {
        assumeKeepAlivePerSocketOptionsSupported();
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.SOCKET_KEEP_IDLE.getName(), "-1");
        assertThrows(IllegalArgumentException.class, () -> newHecatonInstance(config));
        config.setProperty(ClusterProperty.SOCKET_KEEP_IDLE.getName(), "32768");
        assertThrows(IllegalArgumentException.class, () -> newHecatonInstance(config));
        config.setProperty(ClusterProperty.SOCKET_KEEP_IDLE.getName(), "0");
        assertThrows(IllegalArgumentException.class, () -> newHecatonInstance(config));
    }

    @Test
    public void testKeepAliveSocketOptions() throws Throwable {
        assumeKeepAlivePerSocketOptionsSupported();
        Config config = getConfig();

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
    public void testKeepAliveSocketOptions_whenNotSupported() throws Throwable {
        assumeKeepAlivePerSocketOptionsNotSupported();
        // ensure that even though options are configured and setting them fails, no exceptions are thrown
        Config config = getConfig();

        HecatonInstance hz = newHecatonInstance(config);
        newHecatonInstance(config);

        assertClusterSizeEventually(2, hz);
    }

    protected Config getConfig() {
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.SOCKET_KEEP_IDLE.getName(), "5");
        config.setProperty(ClusterProperty.SOCKET_KEEP_INTERVAL.getName(), "1");
        config.setProperty(ClusterProperty.SOCKET_KEEP_COUNT.getName(), "2");
        return config;
    }
}
