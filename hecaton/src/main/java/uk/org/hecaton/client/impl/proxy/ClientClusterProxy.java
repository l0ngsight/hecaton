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

package uk.org.hecaton.client.impl.proxy;

import uk.org.hecaton.client.impl.spi.ClientClusterService;
import uk.org.hecaton.cluster.Cluster;
import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.cluster.MembershipListener;
import uk.org.hecaton.hotrestart.HotRestartService;
import uk.org.hecaton.internal.util.Clock;
import uk.org.hecaton.persistence.PersistenceService;
import uk.org.hecaton.transaction.TransactionOptions;
import uk.org.hecaton.version.Version;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Client implementation of the {@link Cluster}.
 */
public class ClientClusterProxy implements Cluster {

    private final ClientClusterService clusterService;

    public ClientClusterProxy(ClientClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    @Nonnull
    public UUID addMembershipListener(@Nonnull MembershipListener listener) {
        return clusterService.addMembershipListener(listener);
    }

    @Override
    public boolean removeMembershipListener(@Nonnull UUID registrationId) {
        return clusterService.removeMembershipListener(registrationId);
    }

    @Override
    @Nonnull
    public Set<Member> getMembers() {
        final Collection<Member> members = clusterService.getMemberList();
        return new LinkedHashSet<>(members);
    }

    @Override
    @Nonnull
    public Member getLocalMember() {
        throw new UnsupportedOperationException("Client has no local member!");
    }

    @Override
    public long getClusterTime() {
        return Clock.currentTimeMillis();
    }

    @Nonnull
    @Override
    public ClusterState getClusterState() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeClusterState(@Nonnull ClusterState newState) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Nonnull
    public Version getClusterVersion() {
        throw new UnsupportedOperationException();
    }

    @Override
    public HotRestartService getHotRestartService() {
        throw new UnsupportedOperationException();
    }

    @Override
    @Nonnull
    public PersistenceService getPersistenceService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeClusterState(@Nonnull ClusterState newState, @Nonnull TransactionOptions transactionOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shutdown(@Nullable TransactionOptions transactionOptions) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeClusterVersion(@Nonnull Version version) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeClusterVersion(@Nonnull Version version, @Nonnull TransactionOptions options) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void promoteLocalLiteMember() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void demoteLocalDataMember() {
        throw new UnsupportedOperationException();
    }
}
