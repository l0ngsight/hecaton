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

package uk.org.hecaton.internal.loadprotection;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.impl.operationparker.impl.OperationParkerImpl;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;

/** Load protection (F23): client connection lifecycle work on the maintenance lane. */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class MaintenanceLaneTest extends HecatonTestSupport {

    private final List<HecatonInstance> instances = new ArrayList<>();
    private final List<Socket> sockets = new ArrayList<>();

    @After
    public void shutdown() throws IOException {
        for (Socket socket : sockets) {
            socket.close();
        }
        for (int i = instances.size() - 1; i >= 0; i--) {
            instances.get(i).getLifecycleService().terminate();
        }
    }

    private HecatonInstance member(Config config) {
        config.setClusterName("lane-" + UUID.randomUUID());
        config.getNetworkConfig().setPort(5701).setPortAutoIncrement(true);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getJetConfig().setEnabled(false);
        config.getLoadProtectionConfig().setEnabled(true);
        HecatonInstance member = Hecaton.newHecatonInstance(config);
        instances.add(member);
        return member;
    }

    private HecatonInstance client(HecatonInstance member) {
        ClientConfig config = new ClientConfig().setClusterName(member.getConfig().getClusterName());
        config.getNetworkConfig().addAddress("127.0.0.1:" + member.getCluster().getLocalMember().getAddress().getPort());
        HecatonInstance client = HecatonClient.newHecatonClient(config);
        instances.add(client);
        return client;
    }

    private static MaintenanceLane lane(HecatonInstance member) {
        return getNode(member).loadProtection.getMaintenanceLane();
    }

    @Test
    public void disconnectedClientsAreCleanedUpInBatchesOnTheLane() {
        HecatonInstance member = member(smallInstanceConfig());
        List<HecatonInstance> clients = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            HecatonInstance client = client(member);
            client.getMap("m").put(i, i);
            clients.add(client);
        }
        assertThat(getNode(member).clientEngine.getClientEndpointCount()).isEqualTo(10);

        clients.forEach(HecatonInstance::shutdown);

        assertTrueEventually(() -> {
            assertThat(getNode(member).clientEngine.getClientEndpointCount()).isZero();
            assertThat(lane(member).disconnectBatches()).isPositive();
            assertThat(lane(member).backlog()).isZero();
        });
    }

    @Test
    public void theParkedOperationsOfADisconnectedClientAreCancelled() {
        HecatonInstance member = member(smallInstanceConfig());
        HecatonInstance client = client(member);
        OperationParkerImpl parker = (OperationParkerImpl) getNodeEngineImpl(member).getOperationParker();
        new Thread(() -> {
            try {
                client.getQueue("empty").take();
            } catch (Exception ignored) {
                // the client goes away
            }
        }).start();
        assertTrueEventually(() -> assertThat(parker.getTotalParkedOperationCount()).isEqualTo(1));

        client.getLifecycleService().terminate();

        assertTrueEventually(() -> assertThat(parker.getTotalParkedOperationCount()).isZero());
        assertThat(lane(member).disconnectBatches()).isPositive();
    }

    @Test
    public void connectionsAboveTheAcceptRateAreRefused() throws Exception {
        Config config = smallInstanceConfig();
        config.getLoadProtectionConfig().getMaintenanceConfig().setMaxAcceptsPerSecond(2);
        HecatonInstance member = member(config);
        int port = member.getCluster().getLocalMember().getAddress().getPort();

        List<Socket> opened = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Socket socket = new Socket("127.0.0.1", port);
            sockets.add(socket);
            socket.getOutputStream().write("CP2".getBytes(StandardCharsets.UTF_8));
            socket.getOutputStream().flush();
            opened.add(socket);
        }

        assertTrueEventually(() -> assertThat(lane(member).refusedConnections()).isGreaterThanOrEqualTo(8));
        int closedByMember = 0;
        for (Socket socket : opened) {
            if (isClosedByPeer(socket)) {
                closedByMember++;
            }
        }
        assertThat(closedByMember).isGreaterThanOrEqualTo(8);
    }

    private static boolean isClosedByPeer(Socket socket) {
        try {
            socket.setSoTimeout(500);
            InputStream in = socket.getInputStream();
            return in.read() < 0;
        } catch (SocketException e) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
