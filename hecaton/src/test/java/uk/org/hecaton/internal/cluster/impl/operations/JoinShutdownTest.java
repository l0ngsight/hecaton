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

package uk.org.hecaton.internal.cluster.impl.operations;

import uk.org.hecaton.cluster.impl.MemberImpl;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.cluster.impl.ClusterClockImpl;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.internal.cluster.impl.ClusterStateManager;
import uk.org.hecaton.internal.nio.Packet;
import uk.org.hecaton.internal.nio.PacketIOHelper;
import uk.org.hecaton.internal.nio.Protocols;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationAccessor;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.TestAwareInstanceFactory;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collections;

import static uk.org.hecaton.instance.impl.TestUtil.getNode;
import static uk.org.hecaton.instance.impl.TestUtil.toData;
import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;
import static uk.org.hecaton.test.HecatonTestSupport.smallInstanceConfigWithoutJetAndMetrics;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class JoinShutdownTest {

    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();
    private HecatonInstance hz;

    @Parameter
    public boolean joinedBefore;

    @Parameters(name = "joinedBefore:{0}")
    public static Object[] data() {
        return new Object[]{false, true};
    }

    @Before
    public void setUp() throws Exception {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        hz = factory.newHecatonInstance(config);
    }

    @After
    public void tearDown() throws Exception {
        factory.terminateAll();
    }

    @Test
    public void joinedMemberShouldNotShutdown_whenFinalizeJoinOp() throws Exception {
        assertTrueEventually(() -> getNode(hz).getClusterService().isJoined());

        Node node = getNode(hz);
        ClusterServiceImpl clusterService = node.getClusterService();
        ClusterClockImpl clusterClock = clusterService.getClusterClock();
        ClusterStateManager clusterStateManager = clusterService.getClusterStateManager();
        MemberImpl member = clusterService.getLocalMember();
        Operation op = new FinalizeJoinOp(
                member.getUuid(),
                clusterService.getMembershipManager().getMembersView(),
                new OnJoinOp(Collections.emptyList()),
                new OnJoinOp(Collections.emptyList()),
                clusterClock.getClusterTime(),
                clusterService.getClusterId(),
                clusterClock.getClusterStartTime(),
                clusterStateManager.getState(),
                clusterService.getClusterVersion(),
                node.getPartitionService().createPartitionState(),
                false,
                node.getClusterTopologyIntent()
        );
        OperationAccessor.setCallId(op, 1);

        ByteBuffer buffer = toBuffer(op);
        int position = buffer.position();
        buffer.position(286);
        buffer.putInt(42_000_000);
        buffer.position(position);
        buffer.flip();

        if (joinedBefore) {
            node.getClusterService().resetJoinState();
        }
        send(buffer, node.getThisAddress().getInetSocketAddress());
        assertTrue(hz.getLifecycleService().isRunning());
    }

    @Test
    public void joinedMemberShouldNotShutdown_whenConfigMismatchOp() throws Exception {
        Node node = getNode(hz);
        assertTrueEventually(() -> node.getClusterService().isJoined());
        if (joinedBefore) {
            node.getClusterService().resetJoinState();
        }
        send(new ConfigMismatchOp("Random reason"), node.getThisAddress().getInetSocketAddress());
        assertTrue(hz.getLifecycleService().isRunning());
    }

    @Test
    public void joinedMemberShouldNotShutdown_whenBeforeJoinCheckFailureOp() throws Exception {
        Node node = getNode(hz);
        assertTrueEventually(() -> node.getClusterService().isJoined());
        if (joinedBefore) {
            node.getClusterService().resetJoinState();
        }
        send(new BeforeJoinCheckFailureOp("Random reason"), node.getThisAddress().getInetSocketAddress());
        assertTrue(hz.getLifecycleService().isRunning());
    }

    @Test
    public void joinedMemberShouldNotShutdown_whenAuthenticationFailureOp() throws Exception {
        Node node = getNode(hz);
        assertTrueEventually(() -> node.getClusterService().isJoined());
        if (joinedBefore) {
            node.getClusterService().resetJoinState();
        }
        send(new AuthenticationFailureOp(), node.getThisAddress().getInetSocketAddress());
        assertTrue(hz.getLifecycleService().isRunning());
    }

    private static void send(Operation operation, InetSocketAddress address) throws Exception {
        send(toBuffer(operation).flip(), address);
    }

    private static void send(ByteBuffer buffer, InetSocketAddress address) throws Exception {
        // send bytebuffer
        try (SocketChannel socketChannel = SocketChannel.open()) {
            socketChannel.connect(address);
            while (buffer.hasRemaining()) {
                socketChannel.write(buffer);
            }
        }

        // sleep for member to process the operation
        Thread.sleep(100);
    }

    private static ByteBuffer toBuffer(Operation operation) {
        // op -> packet
        Data data = toData(operation);
        Packet packet = new Packet(data.toByteArray());
        packet.setPacketType(Packet.Type.OPERATION);

        //packet -> bytebuffer
        ByteBuffer buffer = ByteBuffer.allocate(packet.getFrameLength() + 3);
        buffer.put(Protocols.CLUSTER.getBytes());
        PacketIOHelper packetIOHelper = new PacketIOHelper();
        packetIOHelper.writeTo(packet, buffer);
        return buffer;
    }
}
