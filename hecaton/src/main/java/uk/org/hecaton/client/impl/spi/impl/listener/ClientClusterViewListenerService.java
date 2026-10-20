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

package uk.org.hecaton.client.impl.spi.impl.listener;

import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.connection.ClientConnection;
import uk.org.hecaton.client.impl.connection.ClientConnectionManager;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.ClientAddClusterViewListenerCodec;
import uk.org.hecaton.client.impl.spi.ClientClusterService;
import uk.org.hecaton.client.impl.spi.ClientListenerService;
import uk.org.hecaton.client.impl.spi.EventHandler;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.client.impl.spi.impl.ClientPartitionServiceImpl;
import uk.org.hecaton.internal.cluster.MemberInfo;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.nio.ConnectionListener;
import uk.org.hecaton.internal.util.ConcurrencyUtil;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.version.Version;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Adds cluster listener to one of the connections. If that connection is removed,
 * it registers connection to any other connection
 */
public class ClientClusterViewListenerService implements ConnectionListener {

    private final HecatonClientInstanceImpl client;
    private final ClientConnectionManager connectionManager;
    private final ClientPartitionServiceImpl partitionService;
    private final ClientClusterService clusterService;
    private final ILogger logger;
    private final AtomicReference<Connection> listenerAddedConnection = new AtomicReference<>();

    public ClientClusterViewListenerService(HecatonClientInstanceImpl client) {
        this.client = client;
        this.logger = client.getLoggingService().getLogger(ClientListenerService.class);
        this.connectionManager = client.getConnectionManager();
        this.partitionService = (ClientPartitionServiceImpl) client.getClientPartitionService();
        this.clusterService =  client.getClientClusterService();
    }

    public void start() {
        connectionManager.addConnectionListener(this);
    }

    // public for tests
    public final class ClusterViewListenerHandler extends ClientAddClusterViewListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final ClientConnection connection;

        private ClusterViewListenerHandler(ClientConnection connection) {
            this.connection = connection;
        }

        @Override
        public void beforeListenerRegister(Connection connection) {
            clusterService.onClusterConnect();
            if (logger.isFinestEnabled()) {
                logger.finest("Register attempt of ClusterViewListenerHandler to %s", connection);
            }
        }

        @Override
        public void onListenerRegister(Connection connection) {
            if (logger.isFinestEnabled()) {
                logger.finest("Registered ClusterViewListenerHandler to %s", connection);
            }
        }

        @Override
        public void handleMembersViewEvent(int memberListVersion, Collection<MemberInfo> memberInfos) {
            clusterService.handleMembersViewEvent(memberListVersion, memberInfos, connection.getClusterUuid());
        }

        @Override
        public void handlePartitionsViewEvent(int version, Collection<Map.Entry<UUID, List<Integer>>> partitions) {
            partitionService.handlePartitionsViewEvent(connection, partitions, version);
        }

        @Override
        public void handleMemberGroupsViewEvent(int version, Collection<Collection<UUID>> memberGroups) {
            clusterService.getSubsetMembers()
                    .updateOnClusterViewEvent(connection.getClusterUuid(), memberGroups, version);
        }

        @Override
        public void handleClusterVersionEvent(Version version) {
            clusterService.handleClusterVersionEvent(version);
        }
    }

    @Override
    public void connectionAdded(Connection connection) {
        tryRegister((ClientConnection) connection);
    }

    @Override
    public void connectionRemoved(Connection connection) {
        tryReregisterToRandomConnection(connection);
    }

    private void tryReregisterToRandomConnection(Connection oldConnection) {
        if (!listenerAddedConnection.compareAndSet(oldConnection, null)) {
            //somebody else already trying to re-register
            return;
        }
        ClientConnection newConnection = connectionManager.getRandomConnection();
        if (newConnection != null) {
            tryRegister(newConnection);
        }
    }

    private void tryRegister(ClientConnection connection) {
        if (!listenerAddedConnection.compareAndSet(null, connection)) {
            //already registering/registered to another connection
            return;
        }
        ClientMessage clientMessage = ClientAddClusterViewListenerCodec.encodeRequest();
        ClientInvocation invocation = new ClientInvocation(client, clientMessage, null, connection);
        ClusterViewListenerHandler handler = new ClusterViewListenerHandler(connection);
        invocation.setEventHandler(handler);
        handler.beforeListenerRegister(connection);
        invocation.invokeUrgent().whenCompleteAsync((message, throwable) -> {
            if (message != null) {
                handler.onListenerRegister(connection);
                return;
            }
            //completes with exception, listener needs to be reregistered
            tryReregisterToRandomConnection(connection);
        }, ConcurrencyUtil.getDefaultAsyncExecutor());
    }

}
