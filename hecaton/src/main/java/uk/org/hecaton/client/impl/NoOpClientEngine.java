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
import java.util.function.LongConsumer;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class NoOpClientEngine implements ClientEngine {

    @Override
    public boolean bind(ClientEndpoint endpoint) {
        return true;
    }

    @Nonnull
    @Override
    public Collection<Client> getClients() {
        return emptyList();
    }

    @Override
    public int getClientEndpointCount() {
        return 0;
    }

    @Override
    public IPartitionService getPartitionService() {
        return null;
    }

    @Override
    public ClusterService getClusterService() {
        return null;
    }

    @Override
    public EventService getEventService() {
        return null;
    }

    @Override
    public ProxyService getProxyService() {
        return null;
    }

    @Override
    public ILogger getLogger(Class clazz) {
        return null;
    }

    @Override
    public Address getThisAddress() {
        return null;
    }

    @Override
    public ClientEndpointManager getEndpointManager() {
        return null;
    }

    @Override
    public ClientExceptionFactory getExceptionFactory() {
        return null;
    }

    @Override
    public SecurityContext getSecurityContext() {
        return null;
    }

    @Override
    public TransactionManagerService getTransactionManagerService() {
        return null;
    }

    @Override
    public ClusterViewListenerService getClusterViewListenerService() {
        return null;
    }

    @Override
    public CPGroupViewListenerService getCPGroupViewListenerService() {
        return null;
    }

    @Override
    public Map<String, Long> getActiveClientsInCluster() {
        return emptyMap();
    }

    @Override
    public Map<UUID, ClientStatistics> getClientStatistics() {
        return emptyMap();
    }

    @Override
    public boolean isClientAllowed(Client client) {
        return true;
    }

    @Override
    public void applySelector(ClientSelector selector) {

    }

    @Override
    public void accept(ClientMessage clientMessage) {

    }

    @Override
    public void onClientAcquiredResource(UUID uuid) {

    }

    @Override
    public void addBackupListener(UUID clientUUID, LongConsumer backupListener) {

    }

    @Override
    public void dispatchBackupEvent(UUID clientUUID, long clientCorrelationId) {

    }

    @Override
    public boolean deregisterBackupListener(UUID clientUUID, LongConsumer backupListener) {
        return false;
    }

    @Override
    public AddressChecker getManagementTasksChecker() {
        return null;
    }
}
