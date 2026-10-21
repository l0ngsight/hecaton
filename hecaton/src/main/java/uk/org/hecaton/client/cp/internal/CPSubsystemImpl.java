/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.client.cp.internal;

import uk.org.hecaton.client.cp.internal.datastructures.proxy.ClientRaftProxyFactory;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CPSubsystemAddGroupAvailabilityListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.CPSubsystemAddMembershipListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.CPSubsystemGetCPGroupIdsCodec;
import uk.org.hecaton.client.impl.protocol.codec.CPSubsystemGetCPObjectInfosCodec;
import uk.org.hecaton.client.impl.protocol.codec.CPSubsystemRemoveGroupAvailabilityListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.CPSubsystemRemoveMembershipListenerCodec;
import uk.org.hecaton.client.impl.spi.ClientContext;
import uk.org.hecaton.client.impl.spi.EventHandler;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocationFuture;
import uk.org.hecaton.client.impl.spi.impl.ListenerMessageCodec;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.cp.CPDataStructureManagementService;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.CPMap;
import uk.org.hecaton.cp.CPMember;
import uk.org.hecaton.cp.CPObjectInfo;
import uk.org.hecaton.cp.CPSubsystem;
import uk.org.hecaton.cp.CPSubsystemManagementService;
import uk.org.hecaton.cp.IAtomicLong;
import uk.org.hecaton.cp.IAtomicReference;
import uk.org.hecaton.cp.ICountDownLatch;
import uk.org.hecaton.cp.ISemaphore;
import uk.org.hecaton.cp.event.CPGroupAvailabilityEvent;
import uk.org.hecaton.cp.event.CPGroupAvailabilityListener;
import uk.org.hecaton.cp.event.CPMembershipEvent;
import uk.org.hecaton.cp.event.CPMembershipEvent.EventType;
import uk.org.hecaton.cp.event.CPMembershipListener;
import uk.org.hecaton.cp.event.impl.CPGroupAvailabilityEventImpl;
import uk.org.hecaton.cp.event.impl.CPMembershipEventImpl;
import uk.org.hecaton.cp.internal.CPObjectInfoImpl;
import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.cp.internal.datastructures.atomiclong.AtomicLongService;
import uk.org.hecaton.cp.internal.datastructures.atomicref.AtomicRefService;
import uk.org.hecaton.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import uk.org.hecaton.cp.internal.datastructures.cpmap.CPMapService;
import uk.org.hecaton.cp.internal.datastructures.lock.LockService;
import uk.org.hecaton.cp.internal.datastructures.semaphore.SemaphoreService;
import uk.org.hecaton.cp.lock.FencedLock;
import uk.org.hecaton.cp.session.CPSessionManagementService;
import uk.org.hecaton.internal.util.Clock;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static uk.org.hecaton.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;

/**
 * Client-side impl of the CP subsystem to create CP data structure proxies
 */
public class CPSubsystemImpl implements CPSubsystem, Consumer<ClientContext> {

    protected final ClientRaftProxyFactory proxyFactory;
    private volatile ClientContext context;

    public CPSubsystemImpl(ClientRaftProxyFactory proxyFactory) {
        this.proxyFactory = proxyFactory;
    }

    public void init(ClientContext context) {
        this.context = context;
        proxyFactory.init(context);
    }

    @Override
    public CPDataStructureManagementService getCPDataStructureManagementService() {
        return (mapName, age) -> CompletableFuture.failedFuture(
                new UnsupportedOperationException("CPMap purge is not available to clients: the client protocol "
                        + "has no message for it. Call it on a member."));
    }

    /** Called by the client once it has started. */
    @Override
    public void accept(ClientContext context) {
        init(context);
    }

    @Nonnull
    @Override
    public IAtomicLong getAtomicLong(@Nonnull String name) {
        checkNotNull(name, "Retrieving an atomic long instance with a null name is not allowed!");
        return proxyFactory.createProxy(AtomicLongService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public <E> IAtomicReference<E> getAtomicReference(@Nonnull String name) {
        checkNotNull(name, "Retrieving an atomic reference instance with a null name is not allowed!");
        return proxyFactory.createProxy(AtomicRefService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public ICountDownLatch getCountDownLatch(@Nonnull String name) {
        checkNotNull(name, "Retrieving a count down latch instance with a null name is not allowed!");
        return proxyFactory.createProxy(CountDownLatchService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public FencedLock getLock(@Nonnull String name) {
        checkNotNull(name, "Retrieving an fenced lock instance with a null name is not allowed!");
        return proxyFactory.createProxy(LockService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public ISemaphore getSemaphore(@Nonnull String name) {
        checkNotNull(name, "Retrieving a semaphore instance with a null name is not allowed!");
        return proxyFactory.createProxy(SemaphoreService.SERVICE_NAME, name);
    }

    @Override
    public CPMember getLocalCPMember() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CPSubsystemManagementService getCPSubsystemManagementService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CPSessionManagementService getCPSessionManagementService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public UUID addMembershipListener(CPMembershipListener listener) {
        return context.getListenerService()
                .registerListener(new CPMembershipListenerMessageCodec(), new CPMembershipEventHandler(listener));
    }

    @Override
    public boolean removeMembershipListener(UUID id) {
        return context.getListenerService().deregisterListener(id);
    }

    @Override
    public UUID addGroupAvailabilityListener(CPGroupAvailabilityListener listener) {
        return context.getListenerService()
                .registerListener(new CPGroupAvailabilityListenerMessageCodec(), new CPGroupAvailabilityEventHandler(listener));
    }

    @Override
    public boolean removeGroupAvailabilityListener(UUID id) {
        return context.getListenerService().deregisterListener(id);
    }

    @Override
    public <K, V> CPMap<K, V> getMap(@Nonnull String name) {
        checkNotNull(name, "Retrieving a CPMap instance with a null name is not allowed!");
        return proxyFactory.createProxy(CPMapService.SERVICE_NAME, name);
    }

    @Nonnull
    @Override
    public Collection<CPGroupId> getCPGroupIds() {
        ClientMessage clientMessage = CPSubsystemGetCPGroupIdsCodec.encodeRequest();
        ClientInvocation invocation = new ClientInvocation(
                (HecatonClientInstanceImpl) context.getHecatonInstance(),
                clientMessage, null
        );
        ClientInvocationFuture future = invocation.invoke();
        try {
            return future.thenApplyAsync(cm ->
                    List.<CPGroupId>copyOf(CPSubsystemGetCPGroupIdsCodec.decodeResponse(cm)),
                    CALLER_RUNS
            ).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new HecatonException("Could not retrieve CP group ids", e);
        }
    }

    @Nonnull
    @Override
    public Iterable<CPObjectInfo> getObjectInfos(@Nonnull CPGroupId groupId, @Nonnull String serviceName) {
        return internalGetObjectInfos(groupId, serviceName, false);
    }

    @Nonnull
    @Override
    public Iterable<CPObjectInfo> getTombstoneInfos(@Nonnull CPGroupId groupId, @Nonnull String serviceName) {
        return internalGetObjectInfos(groupId, serviceName, true);
    }

    private List<CPObjectInfo> internalGetObjectInfos(CPGroupId groupId, String serviceName, boolean tombstone) {
        ClientMessage clientMessage = CPSubsystemGetCPObjectInfosCodec.encodeRequest(
                (RaftGroupId) groupId,
                serviceName,
                tombstone
        );
        ClientInvocation invocation = new ClientInvocation(
                (HecatonClientInstanceImpl) context.getHecatonInstance(),
                clientMessage,
                null
        );
        ClientInvocationFuture future = invocation.invoke();
        try {
            return future.thenApplyAsync(CPSubsystemGetCPObjectInfosCodec::decodeResponse, CALLER_RUNS)
                    .get()
                    .stream()
                    .map(name -> (CPObjectInfo) new CPObjectInfoImpl(name, serviceName, groupId))
                    .toList();
        } catch (InterruptedException | ExecutionException e) {
            throw new HecatonException("Could not retrieve CP object infos", e);
        }
    }

    private static class CPMembershipEventHandler extends CPSubsystemAddMembershipListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final CPMembershipListener listener;

        CPMembershipEventHandler(CPMembershipListener listener) {
            this.listener = listener;
        }

        @Override
        public void handleMembershipEventEvent(CPMember member, byte type) {
            CPMembershipEvent event = new CPMembershipEventImpl(member, type);
            if (event.getType() == EventType.ADDED) {
                listener.memberAdded(event);
            } else {
                listener.memberRemoved(event);
            }
        }
    }

    private static class CPGroupAvailabilityEventHandler extends CPSubsystemAddGroupAvailabilityListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private static final long DEDUPLICATION_PERIOD = TimeUnit.MINUTES.toMillis(1);
        private final CPGroupAvailabilityListener listener;
        private final Map<CPGroupAvailabilityEvent, Long> recentEvents = new ConcurrentHashMap<>();

        CPGroupAvailabilityEventHandler(CPGroupAvailabilityListener listener) {
            this.listener = listener;
        }

        @Override
        public void handleGroupAvailabilityEventEvent(RaftGroupId groupId, Collection<CPMember> members,
                                                      Collection<CPMember> unavailableMembers, boolean isIsShutdownExists,
                                                      boolean isShutdown) {
            boolean isShutdownValue = isIsShutdownExists && isShutdown;
            handleGroupAvailabilityEventEvent(groupId, members, unavailableMembers, isShutdownValue);
        }

        public void handleGroupAvailabilityEventEvent(RaftGroupId groupId, Collection<CPMember> members,
                                                      Collection<CPMember> unavailableMembers, boolean isShutdown) {

            long now = Clock.currentTimeMillis();
            recentEvents.values().removeIf(expirationTime -> expirationTime < now);

            CPGroupAvailabilityEvent event = new CPGroupAvailabilityEventImpl(groupId, members, unavailableMembers, isShutdown);
            if (recentEvents.putIfAbsent(event, now + DEDUPLICATION_PERIOD) != null) {
                return;
            }

            if (event.isMajorityAvailable()) {
                listener.availabilityDecreased(event);
            } else {
                listener.majorityLost(event);
            }
        }
    }

    private static class CPMembershipListenerMessageCodec implements ListenerMessageCodec {
        @Override
        public ClientMessage encodeAddRequest(boolean localOnly) {
            return CPSubsystemAddMembershipListenerCodec.encodeRequest(localOnly);
        }

        @Override
        public UUID decodeAddResponse(ClientMessage clientMessage) {
            return CPSubsystemAddMembershipListenerCodec.decodeResponse(clientMessage);
        }

        @Override
        public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
            return CPSubsystemRemoveMembershipListenerCodec.encodeRequest(realRegistrationId);
        }

        @Override
        public boolean decodeRemoveResponse(ClientMessage clientMessage) {
            return CPSubsystemRemoveMembershipListenerCodec.decodeResponse(clientMessage);
        }
    }

    private static class CPGroupAvailabilityListenerMessageCodec implements ListenerMessageCodec {
        @Override
        public ClientMessage encodeAddRequest(boolean localOnly) {
            return CPSubsystemAddGroupAvailabilityListenerCodec.encodeRequest(localOnly);
        }

        @Override
        public UUID decodeAddResponse(ClientMessage clientMessage) {
            return CPSubsystemAddGroupAvailabilityListenerCodec.decodeResponse(clientMessage);
        }

        @Override
        public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
            return CPSubsystemRemoveGroupAvailabilityListenerCodec.encodeRequest(realRegistrationId);
        }

        @Override
        public boolean decodeRemoveResponse(ClientMessage clientMessage) {
            return CPSubsystemRemoveGroupAvailabilityListenerCodec.decodeResponse(clientMessage);
        }
    }
}
