/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.nio;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.SocketInterceptorConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Socket interceptors (F5) on members and clients: a connecting side sends a token, the accepting
 * side checks it before the protocol starts.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class SocketInterceptorTest extends HecatonTestSupport {

    private final List<HecatonInstance> instances = new ArrayList<>();

    @After
    public void tearDown() {
        instances.forEach(instance -> instance.getLifecycleService().terminate());
        TokenInterceptor.ACCEPTED.set(0);
        TokenInterceptor.REJECTED.set(0);
    }

    @Test
    public void membersAndClientsWithTheTokenConnect() {
        int[] ports = {freePort(), freePort()};
        HecatonInstance first = start(memberConfig(ports, 0, "secret"));
        HecatonInstance second = start(memberConfig(ports, 1, "secret"));
        assertClusterSizeEventually(2, first, second);

        HecatonInstance client = startClient(ports[0], "secret");
        client.getMap("m").put(1, 1);
        assertThat(second.getMap("m").get(1)).isEqualTo(1);
        assertThat(TokenInterceptor.ACCEPTED.get()).isGreaterThanOrEqualTo(2);
        assertThat(TokenInterceptor.REJECTED.get()).isZero();
    }

    @Test
    public void connectionsWithoutTheTokenAreRejected() {
        int[] ports = {freePort(), freePort()};
        HecatonInstance first = start(memberConfig(ports, 0, "secret"));
        HecatonInstance other = start(memberConfig(ports, 1, "other"));
        sleepSeconds(5);
        assertThat(first.getCluster().getMembers()).hasSize(1);
        assertThat(other.getCluster().getMembers()).hasSize(1);

        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1:" + ports[0]);
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(3000);
        assertThatThrownBy(() -> instances.add(HecatonClient.newHecatonClient(config)))
                .isInstanceOf(IllegalStateException.class);
        assertThat(TokenInterceptor.REJECTED.get()).isPositive();
    }

    private HecatonInstance start(Config config) {
        HecatonInstance instance = Hecaton.newHecatonInstance(config);
        instances.add(instance);
        return instance;
    }

    private HecatonInstance startClient(int port, String token) {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1:" + port);
        config.getNetworkConfig().setSocketInterceptorConfig(interceptor(token));
        HecatonInstance client = HecatonClient.newHecatonClient(config);
        instances.add(client);
        return client;
    }

    private static Config memberConfig(int[] ports, int index, String token) {
        Config config = smallInstanceConfig();
        config.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config.getNetworkConfig().setPort(ports[index]).setPortAutoIncrement(false)
                .setSocketInterceptorConfig(interceptor(token));
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        for (int port : ports) {
            join.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1:" + port);
        }
        return config;
    }

    private static SocketInterceptorConfig interceptor(String token) {
        return new SocketInterceptorConfig().setEnabled(true).setClassName(TokenInterceptor.class.getName())
                .setProperty("token", token);
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Sends a token on connect, checks it on accept. */
    public static class TokenInterceptor implements MemberSocketInterceptor {
        static final AtomicInteger ACCEPTED = new AtomicInteger();
        static final AtomicInteger REJECTED = new AtomicInteger();

        private byte token;

        @Override
        public void init(Properties properties) {
            token = (byte) properties.getProperty("token").hashCode();
        }

        @Override
        public void onConnect(Socket connectedSocket) throws IOException {
            connectedSocket.getOutputStream().write(token);
            connectedSocket.getOutputStream().flush();
        }

        @Override
        public void onAccept(Socket acceptedSocket) throws IOException {
            int received = acceptedSocket.getInputStream().read();
            if (received != (token & 0xFF)) {
                REJECTED.incrementAndGet();
                throw new IOException("wrong token");
            }
            ACCEPTED.incrementAndGet();
        }
    }
}
