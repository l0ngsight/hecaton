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

package uk.org.hecaton.client.test;

import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.impl.connection.ClientConnection;
import uk.org.hecaton.client.impl.connection.ClientConnectionManager;
import uk.org.hecaton.client.impl.spi.EventHandler;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.LifecycleEvent;
import uk.org.hecaton.core.LifecycleListener;
import uk.org.hecaton.test.HecatonTestSupport;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ClientTestSupport extends HecatonTestSupport {

    /**
     * Blocks incoming messages to client from given instance
     */
    protected void blockMessagesFromInstance(HecatonInstance instance, HecatonInstance client) {
        HecatonClientInstanceImpl clientImpl = getHecatonClientInstanceImpl(client);
        ClientConnectionManager connectionManager = clientImpl.getConnectionManager();
        Address address = instance.getCluster().getLocalMember().getAddress();
        ((TestClientRegistry.MockTcpClientConnectionManager) connectionManager).blockFrom(address);
    }

    /**
     * Unblocks incoming messages to client from given instance
     */
    protected void unblockMessagesFromInstance(HecatonInstance instance, HecatonInstance client) {
        HecatonClientInstanceImpl clientImpl = getHecatonClientInstanceImpl(client);
        ClientConnectionManager connectionManager = clientImpl.getConnectionManager();
        Address address = instance.getCluster().getLocalMember().getAddress();
        ((TestClientRegistry.MockTcpClientConnectionManager) connectionManager).unblockFrom(address);
    }

    /**
     * Blocks outgoing messages from client to given instance
     */
    protected void blockMessagesToInstance(HecatonInstance instance, HecatonInstance client) {
        HecatonClientInstanceImpl clientImpl = getHecatonClientInstanceImpl(client);
        ClientConnectionManager connectionManager = clientImpl.getConnectionManager();
        Address address = instance.getCluster().getLocalMember().getAddress();
        ((TestClientRegistry.MockTcpClientConnectionManager) connectionManager).blockTo(address);
    }

    /**
     * Unblocks outgoing messages from client to given instance
     */
    protected void unblockMessagesToInstance(HecatonInstance instance, HecatonInstance client) {
        HecatonClientInstanceImpl clientImpl = getHecatonClientInstanceImpl(client);
        ClientConnectionManager connectionManager = clientImpl.getConnectionManager();
        Address address = instance.getCluster().getLocalMember().getAddress();
        ((TestClientRegistry.MockTcpClientConnectionManager) connectionManager).unblockTo(address);
    }

    protected static HecatonClientInstanceImpl getHecatonClientInstanceImpl(HecatonInstance client) {
        if (client instanceof HecatonClientInstanceImpl impl) {
            return impl;
        }
        HecatonClientProxy clientProxy = (HecatonClientProxy) client;
        return clientProxy.client;
    }

    public static void makeSureDisconnectedFromServer(final HecatonInstance client, UUID memberUUID) {
        assertTrueEventually(() -> {
            ClientConnectionManager connectionManager = getHecatonClientInstanceImpl(client).getConnectionManager();
            assertNull(connectionManager.getActiveConnection(memberUUID));
        });
    }

    public static void makeSureConnectedToServers(final HecatonInstance client,
                                               final int expectedConnectedServerCount) {
        assertTrueEventually(() -> {
            assert client != null;
            HecatonClientInstanceImpl clientInstanceImpl = getHecatonClientInstanceImpl(client);
            ClientConnectionManager connectionManager = clientInstanceImpl.getConnectionManager();
            Collection<ClientConnection> activeConnections = connectionManager.getActiveConnections();
            assertEquals(activeConnections.toString(), expectedConnectedServerCount, activeConnections.size());
        });
    }

    protected Map<Long, EventHandler> getAllEventHandlers(HecatonInstance client) {
        ClientConnectionManager connectionManager = getHecatonClientInstanceImpl(client).getConnectionManager();
        Collection<ClientConnection> activeConnections = connectionManager.getActiveConnections();
        HashMap<Long, EventHandler> map = new HashMap<>();
        for (ClientConnection activeConnection : activeConnections) {
            map.putAll(activeConnection.getEventHandlers());
        }
        return map;
    }

    public static class ReconnectListener implements LifecycleListener {

        public final CountDownLatch disconnectedLatch = new CountDownLatch(1);
        public final CountDownLatch reconnectedLatch = new CountDownLatch(1);
        private final AtomicBoolean disconnected = new AtomicBoolean();

        @Override
        public void stateChanged(LifecycleEvent event) {
            LifecycleEvent.LifecycleState state = event.getState();
            if (state == LifecycleEvent.LifecycleState.CLIENT_DISCONNECTED) {
                disconnected.set(true);
                disconnectedLatch.countDown();
            } else if (state == LifecycleEvent.LifecycleState.CLIENT_CONNECTED) {
                if (disconnected.get()) {
                    reconnectedLatch.countDown();
                }
            }
        }
    }
}
