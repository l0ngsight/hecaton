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

package uk.org.hecaton.client.impl;

import uk.org.hecaton.client.Client;
import uk.org.hecaton.client.impl.protocol.ClientExceptionFactory;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.statistics.ClientStatistics;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.internal.cluster.AddressChecker;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.loadprotection.ClientAdmission;
import uk.org.hecaton.internal.loadprotection.MaintenanceLane;
import uk.org.hecaton.internal.nio.ConnectionType;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.security.SecurityContext;
import uk.org.hecaton.spi.impl.eventservice.EventService;
import uk.org.hecaton.spi.impl.proxyservice.ProxyService;
import uk.org.hecaton.transaction.TransactionManagerService;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * The client Engine.
 * <p>
 * todo: what is the purpose of the client engine.
 */
public interface ClientEngine extends Consumer<ClientMessage> {

    /** The admission of client requests (load protection, F23), or {@code null} when load protection is off. */
    default ClientAdmission getClientAdmission() {
        return null;
    }

    /** The lane for client connection lifecycle work (load protection, F23), or {@code null} when it is off. */
    default MaintenanceLane getMaintenanceLane() {
        return null;
    }

    /**
     * Registers client endpoint to endpointManager.
     * Only authenticated endpoints should be registered here.
     * bind can be called twice for same connection, as long as client is allowed to be registered all calls to this
     * method returns true
     * <p>
     * A selector could prevent endpoint to be registered
     * see {@link #applySelector}
     *
     * @param endpoint to be registered to client engine
     * @return false if client is not allowed to join because of a selector, true otherwise
     */
    boolean bind(ClientEndpoint endpoint);

    @Nonnull
    Collection<Client> getClients();

    int getClientEndpointCount();

    IPartitionService getPartitionService();

    ClusterService getClusterService();

    EventService getEventService();

    ProxyService getProxyService();

    ILogger getLogger(Class clazz);

    /**
     * @return the address of this member that listens for CLIENT protocol connections. When advanced network config
     * is in use, it will be different from the MEMBER listening address reported eg by {@code Node.getThisAddress()}
     */
    Address getThisAddress();

    ClientEndpointManager getEndpointManager();

    ClientExceptionFactory getExceptionFactory();

    SecurityContext getSecurityContext();

    TransactionManagerService getTransactionManagerService();

    ClusterViewListenerService getClusterViewListenerService();

    /**
     * Retrieves the {@link CPGroupViewListenerService} which maintains
     * a view of CP leaders based on events received by cluster members.
     * This service is called internally when license requirements are met
     * to provide direct-to-leader CP operation sending.
     *
     * @return the {@link CPGroupViewListenerService} implementation
     */
    CPGroupViewListenerService getCPGroupViewListenerService();

    /**
     * Returns the map of the active clients connected to
     * the cluster. Mapping is from the client type to
     * client count.
     */
    Map<String, Long> getActiveClientsInCluster();

    /**
     * Returns the latest client statistics mapped to the client UUIDs.
     *
     * @return map of the client statistics
     */
    Map<UUID, ClientStatistics> getClientStatistics();

    /**
     * @param client to check if allowed through current ClientSelector.
     *               <p>
     *               Note: Management Center clients
     *               ({@link ConnectionType#MC_JAVA_CLIENT} and {@link ConnectionType#MC_CL_CLIENT}) are always allowed.
     * @return true if allowed, false otherwise
     */
    boolean isClientAllowed(Client client);

    /**
     * Only Clients that can pass through filter are allowed to connect to cluster.
     * Only one selector can be active at a time. Applying new one will override old selector.
     * <p>
     * Note: the only exception to this rule are Management Center clients
     * ({@link ConnectionType#MC_JAVA_CLIENT} and {@link ConnectionType#MC_CL_CLIENT}).
     *
     * @param selector to select a client or group of clients to act upon
     */
    void applySelector(ClientSelector selector);

    /**
     * Notify client engine that a client with given uuid required a resource (lock) on this member
     *
     * @param uuid client uuid
     */
    void onClientAcquiredResource(UUID uuid);

    void addBackupListener(UUID clientUUID, LongConsumer backupListener);

    boolean deregisterBackupListener(UUID clientUUID, LongConsumer backupListener);

    void dispatchBackupEvent(UUID clientUUID, long clientCorrelationId);

    AddressChecker getManagementTasksChecker();
}
