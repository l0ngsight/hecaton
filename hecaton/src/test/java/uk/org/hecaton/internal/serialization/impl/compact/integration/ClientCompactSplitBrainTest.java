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

package uk.org.hecaton.internal.serialization.impl.compact.integration;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.clientside.ClientSchemaService;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.connection.tcp.TcpClientConnection;
import uk.org.hecaton.client.impl.spi.EventHandler;
import uk.org.hecaton.client.impl.spi.impl.listener.ClientClusterViewListenerService;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.impl.compact.CompactTestUtil;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.nio.serialization.HecatonSerializationException;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import example.serialization.EmployeeDTO;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.client.properties.ClientProperty.INVOCATION_RETRY_PAUSE_MILLIS;
import static uk.org.hecaton.instance.impl.TestUtil.getNode;
import static uk.org.hecaton.test.SplitBrainTestSupport.blockCommunicationBetween;
import static uk.org.hecaton.test.SplitBrainTestSupport.unblockCommunicationBetween;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientCompactSplitBrainTest extends ClientTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testLocalSchemasAreSent_whenClientReconnectsToOtherHalf() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();

        HecatonInstance instance1 = factory.newHecatonInstance(config);
        HecatonInstance instance2 = factory.newHecatonInstance(config);

        assertClusterSizeEventually(2, instance1, instance2);

        // split the cluster
        blockCommunicationBetween(instance1, instance2);

        // make sure that each member quickly drops the other from their member list
        closeConnectionBetween(instance1, instance2);

        assertClusterSizeEventually(1, instance1);
        assertClusterSizeEventually(1, instance2);

        HecatonInstance client = factory.newHecatonClient();
        Set<Member> members = client.getCluster().getMembers();
        assertEquals(1, members.size());

        HecatonInstance connectedMember;
        if (members.iterator().next().getUuid().equals(instance1.getLocalEndpoint().getUuid())) {
            connectedMember = instance1;
        } else {
            connectedMember = instance2;
        }

        // Put a compact serializable object
        IMap<Integer, EmployeeDTO> map = client.getMap("test");
        map.put(1, new EmployeeDTO(1, 1));

        CompactTestUtil.assertSchemasAvailable(Collections.singletonList(connectedMember), EmployeeDTO.class);

        ReconnectListener reconnectListener = new ReconnectListener();
        client.getLifecycleService().addLifecycleListener(reconnectListener);

        connectedMember.shutdown();

        assertOpenEventually(reconnectListener.reconnectedLatch);

        HecatonInstance reconnectedMember = connectedMember == instance1
                ? instance2
                : instance1;

        // It might take a while until the state is sent from the client to the
        // reconnected member
        assertTrueEventually(() -> {
            CompactTestUtil.assertSchemasAvailable(Collections.singletonList(reconnectedMember), EmployeeDTO.class);
        });
    }

    @Test
    public void testSchemaReplicationRetried_whenClientIsConnectedToBothHalvesOfTheSplit() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();

        HecatonInstance instance1 = factory.newHecatonInstance(config);
        HecatonInstance instance2 = factory.newHecatonInstance(config);

        assertClusterSizeEventually(2, instance1, instance2);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getProperties().setProperty(INVOCATION_RETRY_PAUSE_MILLIS.getName(), "10");
        clientConfig.getProperties().setProperty(ClientSchemaService.MAX_PUT_RETRY_COUNT.getName(), "50");
        HecatonInstance client = factory.newHecatonClient(clientConfig);
        Set<Member> members = client.getCluster().getMembers();
        assertEquals(2, members.size());

        // Replace the cluster view event handler to delay executing member list
        // events in the client-side
        CountDownLatch latch = replaceClusterViewEventHandler(client);

        // split the cluster
        blockCommunicationBetween(instance1, instance2);

        // make sure that each member quickly drops the other from their member list
        closeConnectionBetween(instance1, instance2);

        IMap<Integer, EmployeeDTO> map = client.getMap("test");

        assertThatThrownBy(() -> {
            map.put(1, new EmployeeDTO(1, 1));
        }).isInstanceOf(HecatonSerializationException.class)
                .hasRootCauseExactlyInstanceOf(IllegalStateException.class)
                .hasStackTraceContaining("after 50 retries")
                .hasStackTraceContaining("connected to the two halves of the cluster");

        // Now the handler can run member list events
        latch.countDown();

        unblockCommunicationBetween(instance1, instance2);
        mergeClusters(instance1, instance2);

        assertClusterSizeEventually(2, instance1, instance2);

        map.put(1, new EmployeeDTO(1, 1));

        CompactTestUtil.assertSchemasAvailable(Arrays.asList(instance1, instance2), EmployeeDTO.class);
    }

    private void mergeClusters(HecatonInstance instance1, HecatonInstance instance2) {
        final Node node1 = getNode(instance1);
        final Node node2 = getNode(instance2);

        final ClusterServiceImpl clusterService = node1.getClusterService();
        clusterService.merge(node2.address);
    }

    private CountDownLatch replaceClusterViewEventHandler(HecatonInstance instance) {
        CountDownLatch latch = new CountDownLatch(1);

        HecatonClientInstanceImpl client = getHecatonClientInstanceImpl(instance);
        for (Connection connection : client.getConnectionManager().getActiveConnections()) {
            TcpClientConnection clientConnection = (TcpClientConnection) connection;
            Map<Long, EventHandler> handlers = clientConnection.getEventHandlers();
            for (Map.Entry<Long, EventHandler> handlerEntry : handlers.entrySet()) {
                Long correlationId = handlerEntry.getKey();
                EventHandler handler = handlerEntry.getValue();
                if (handler instanceof ClientClusterViewListenerService.ClusterViewListenerHandler) {
                    AwaitingEventHandler newHandler = new AwaitingEventHandler(handler, latch);
                    clientConnection.addEventHandler(correlationId, newHandler);
                    return latch;
                }
            }
        }

        throw new IllegalStateException("There must be a cluster view listener added to a connection.");
    }

    private static final class AwaitingEventHandler<E> implements EventHandler<E> {

        private final EventHandler<E> delegate;
        private final CountDownLatch latch;

        AwaitingEventHandler(EventHandler<E> delegate, CountDownLatch latch) {
            this.delegate = delegate;
            this.latch = latch;
        }

        @Override
        public void handle(E event) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            delegate.handle(event);
        }
    }
}
