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
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.ClientMessageReader;
import uk.org.hecaton.client.impl.protocol.ClientMessageWriter;
import uk.org.hecaton.client.impl.protocol.codec.ClientAuthenticationCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapGetCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapPutCodec;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonOverloadException;
import uk.org.hecaton.internal.networking.nio.ConnectionBufferPool;
import uk.org.hecaton.internal.nio.Protocols;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static uk.org.hecaton.test.Accessors.getNode;

/** Load protection (F23) on real connections: admission, outbound accounting and socket buffers. */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class LoadProtectionNetworkTest extends HecatonTestSupport {

    private static final int VALUE_SIZE = 1 << 20;

    private final List<HecatonInstance> instances = new ArrayList<>();
    private final List<Socket> sockets = new ArrayList<>();
    private final InternalSerializationService serialization = new DefaultSerializationServiceBuilder().build();

    @After
    public void shutdown() throws IOException {
        for (Socket socket : sockets) {
            socket.close();
        }
        for (int i = instances.size() - 1; i >= 0; i--) {
            instances.get(i).shutdown();
        }
    }

    private HecatonInstance member(Config config) {
        config.setClusterName("lp-" + UUID.randomUUID());
        config.getNetworkConfig().setPort(5701).setPortAutoIncrement(true);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getJetConfig().setEnabled(false);
        config.getLoadProtectionConfig().setEnabled(true);
        config.addMapConfig(new MapConfig("catalog").setImmutable(true));
        HecatonInstance member = Hecaton.newHecatonInstance(config);
        instances.add(member);
        return member;
    }

    private static ClientAdmission admission(HecatonInstance member) {
        return getNode(member).loadProtection.getClientAdmission();
    }

    @Test
    public void aClientThatDoesNotReadIsHeldBackCountedOnceAndDisconnected() throws Exception {
        Config config = smallInstanceConfig();
        config.getLoadProtectionConfig().getOutboundConfig()
                .setMaxPendingBytesPerConnection(8L << 20)
                .setSlowConsumerTimeoutSeconds(2);
        HecatonInstance member = member(config);
        member.getMap("catalog").put("hot", new byte[VALUE_SIZE]);
        ClientAdmission admission = admission(member);

        RawClient client = connect(member);
        int requests = 200;
        for (int i = 0; i < requests; i++) {
            client.send(get("hot", member), 100 + i);
        }

        // the member answers some, stops reading from the connection, and holds the shared value once
        assertTrueEventually(() -> assertThat(admission.readSuspensions()).isPositive());
        assertTrueEventually(() -> assertThat(admission.sharedLogicalBytes()).isGreaterThan(2L * VALUE_SIZE));
        assertThat(admission.physicalBytes()).isLessThanOrEqualTo(VALUE_SIZE + 64);

        // then closes it, since it does not take what is queued for it
        assertTrueEventually(() -> assertThat(admission.slowConsumerDisconnects()).isPositive());
        assertTrueEventually(() -> {
            assertThat(admission.physicalBytes()).isZero();
            assertThat(admission.inFlight()).isZero();
        });
    }

    @Test
    public void anAuthenticatedClientCannotSendAMessageAboveTheLimit() throws Exception {
        Config config = smallInstanceConfig();
        config.getLoadProtectionConfig().getAdmissionConfig().setMaxMessageBytes(64 << 10);
        HecatonInstance member = member(config);
        RawClient client = connect(member);

        Data key = serialization.toData("big");
        ClientMessage put = MapPutCodec.encodeRequest("plain", key, serialization.toData(new byte[256 << 10]), 1, -1);
        put.setPartitionId(member.getPartitionService().getPartition("big").getPartitionId());
        client.send(put, 2);

        // the member closes the connection instead of reading the message
        assertTrueEventually(() -> assertThat(client.isClosedByPeer()).isTrue());
    }

    @Test
    public void requestsAreRefusedWhileTheHeapIsAboveTheThreshold() {
        Config config = smallInstanceConfig();
        config.getLoadProtectionConfig().getAdmissionConfig().setHeapRejectThreshold(0.000001f);
        HecatonInstance member = member(config);
        ClientConfig clientConfig = new ClientConfig().setClusterName(member.getConfig().getClusterName());
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:" + member.getCluster().getLocalMember().getAddress().getPort());
        HecatonInstance client = HecatonClient.newHecatonClient(clientConfig);
        instances.add(client);
        IMap<String, String> map = client.getMap("catalog");

        assertTrueEventually(() -> {
            System.gc();
            assertThatThrownBy(() -> map.get("any")).isInstanceOf(HecatonOverloadException.class);
        });
        assertThat(admission(member).overloadRejections()).isPositive();
    }

    @Test
    public void idleClientConnectionsHoldNoPooledSocketBuffers() {
        HecatonInstance member = member(smallInstanceConfig());
        ClientConfig clientConfig = new ClientConfig().setClusterName(member.getConfig().getClusterName());
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:" + member.getCluster().getLocalMember().getAddress().getPort());
        int clients = 8;
        for (int i = 0; i < clients; i++) {
            HecatonInstance client = HecatonClient.newHecatonClient(clientConfig);
            instances.add(client);
            client.getMap("catalog").put("key-" + i, "value");
        }

        // between requests and heartbeats every connection hands its socket read and write buffers back
        int bufferBytes = member.getConfig().getLoadProtectionConfig().getConnectionBufferConfig().getClientBufferBytes();
        ConnectionBufferPool pool = admission(member).bufferPool();
        assertTrueEventually(() -> {
            assertThat(pool.bytesInUse()).isZero();
            assertThat(pool.bytesIdle()).isGreaterThanOrEqualTo(2L * clients * bufferBytes);
        });
    }

    private ClientMessage get(String key, HecatonInstance member) {
        ClientMessage get = MapGetCodec.encodeRequest("catalog", serialization.toData(key), 1);
        get.setPartitionId(member.getPartitionService().getPartition(key).getPartitionId());
        return get;
    }

    private RawClient connect(HecatonInstance member) throws IOException {
        Socket socket = new Socket("127.0.0.1", member.getCluster().getLocalMember().getAddress().getPort());
        sockets.add(socket);
        RawClient client = new RawClient(socket);
        client.out.write(Protocols.CLIENT_BINARY.getBytes(StandardCharsets.UTF_8));
        client.send(ClientAuthenticationCodec.encodeRequest(member.getConfig().getClusterName(), null, null,
                UUID.randomUUID(), "JVM", InternalSerializationService.VERSION_1, "5.7.0", "raw", List.of(),
                (byte) 1, false), 1);
        ClientMessage response = client.read();
        assertThat(ClientAuthenticationCodec.decodeResponse(response).status).isZero();
        return client;
    }

    /** A client that speaks the client protocol on a plain socket and reads only when asked. */
    private static final class RawClient {
        private final Socket socket;
        private final OutputStream out;
        private final InputStream in;
        private final ByteBuffer inBuffer = ByteBuffer.allocate(64 << 10);

        RawClient(Socket socket) throws IOException {
            this.socket = socket;
            this.out = socket.getOutputStream();
            this.in = socket.getInputStream();
            inBuffer.flip();
        }

        void send(ClientMessage message, long correlationId) throws IOException {
            message.setCorrelationId(correlationId);
            ClientMessageWriter writer = new ClientMessageWriter();
            ByteBuffer buffer = ByteBuffer.allocate(64 << 10);
            boolean done;
            do {
                buffer.clear();
                done = writer.writeTo(buffer, message);
                buffer.flip();
                out.write(buffer.array(), 0, buffer.limit());
            } while (!done);
            out.flush();
        }

        ClientMessage read() throws IOException {
            ClientMessageReader reader = new ClientMessageReader(-1);
            while (!reader.readFrom(inBuffer, true)) {
                inBuffer.compact();
                int read = in.read(inBuffer.array(), inBuffer.position(), inBuffer.remaining());
                if (read < 0) {
                    throw new IOException("closed");
                }
                inBuffer.position(inBuffer.position() + read);
                inBuffer.flip();
            }
            return reader.getClientMessage();
        }

        /** Whether the member closed the connection: reads end, or writes fail. */
        boolean isClosedByPeer() {
            try {
                socket.setSoTimeout(100);
                byte[] bytes = new byte[8192];
                while (true) {
                    int read = in.read(bytes);
                    if (read < 0) {
                        return true;
                    }
                }
            } catch (SocketException e) {
                return true;
            } catch (IOException e) {
                return false;
            }
        }
    }
}
