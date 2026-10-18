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

package uk.org.hecaton.client.impl.protocol.task.replicatedmap;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.task.AbstractAddListenerMessageTask;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.EntryListener;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.map.IMapEvent;
import uk.org.hecaton.map.MapEvent;
import uk.org.hecaton.map.impl.DataAwareEntryEvent;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapEventPublishingService;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapService;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedEntryEventFilter;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedQueryEventFilter;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.UserCodeNamespacePermission;
import uk.org.hecaton.security.permission.ReplicatedMapPermission;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.spi.impl.InternalCompletableFuture.newCompletedFuture;

public abstract class AbstractReplicatedMapAddEntryListenerMessageTask<Parameter>
        extends AbstractAddListenerMessageTask<Parameter>
        implements EntryListener<Object, Object> {

    protected AbstractReplicatedMapAddEntryListenerMessageTask(ClientMessage clientMessage, Node node,
                                                            Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<UUID> processInternal() {
        ReplicatedMapService service = getService(getServiceName());
        ReplicatedMapEventPublishingService eventPublishingService = service.getEventPublishingService();
        Predicate predicate = getPredicate();
        ReplicatedEntryEventFilter filter;
        if (predicate == null) {
            filter = new ReplicatedEntryEventFilter(getKey());
        } else {
            filter = new ReplicatedQueryEventFilter(getKey(), predicate);
        }
        return newCompletedFuture(eventPublishingService.addLocalEventListener(this, filter, getDistributedObjectName()));
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD_ENTRY_LISTENER;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ReplicatedMapPermission(getDistributedObjectName(), ActionConstants.ACTION_LISTEN);
    }

    @Override
    public Permission getUserCodeNamespacePermission() {
        String namespace = ReplicatedMapService.lookupNamespace(nodeEngine, getDistributedObjectName());
        return namespace != null ? new UserCodeNamespacePermission(namespace, ActionConstants.ACTION_USE) : null;
    }

    public abstract Predicate getPredicate();

    public abstract Data getKey();

    protected abstract boolean isLocalOnly();

    private void handleEvent(EntryEvent<Object, Object> event) {
        if (!shouldSendEvent(event)) {
            return;
        }

        DataAwareEntryEvent<Object, Object> dataAwareEntryEvent = (DataAwareEntryEvent<Object, Object>) event;

        Data key = dataAwareEntryEvent.getKeyData();
        Data newValue = dataAwareEntryEvent.getNewValueData();
        Data oldValue = dataAwareEntryEvent.getOldValueData();
        Data mergingValue = dataAwareEntryEvent.getMergingValueData();

        ClientMessage clientMessage = encodeEvent(key
                , newValue, oldValue, mergingValue, event.getEventType().getType(),
                event.getMember().getUuid(), 1);
        sendClientMessage(key, clientMessage);

    }

    private void handleMapEvent(MapEvent event) {
        if (!shouldSendEvent(event)) {
            return;
        }

        ClientMessage clientMessage = encodeEvent(null
                , null, null, null, event.getEventType().getType(),
                event.getMember().getUuid(), event.getNumberOfEntriesAffected());
        sendClientMessage(null, clientMessage);
    }

    private boolean shouldSendEvent(IMapEvent event) {
        if (!endpoint.isAlive()) {
            return false;
        }

        Member originatedMember = event.getMember();
        if (isLocalOnly() && !nodeEngine.getLocalMember().equals(originatedMember)) {
            //if listener is registered local only, do not let the events originated from other members pass through
            return false;
        }
        return true;
    }


    protected abstract ClientMessage encodeEvent(Data key, Data newValue, Data oldValue,
                                                 Data mergingValue, int type, UUID uuid, int numberOfAffectedEntries);

    @Override
    public void entryAdded(EntryEvent<Object, Object> event) {
        handleEvent(event);
    }

    @Override
    public void entryRemoved(EntryEvent<Object, Object> event) {
        handleEvent(event);
    }

    @Override
    public void entryUpdated(EntryEvent<Object, Object> event) {
        handleEvent(event);
    }

    @Override
    public void entryEvicted(EntryEvent<Object, Object> event) {
        handleEvent(event);
    }

    @Override
    public void entryExpired(EntryEvent<Object, Object> event) {
        handleEvent(event);
    }

    @Override
    public void mapEvicted(MapEvent event) {
        handleMapEvent(event);
    }

    @Override
    public void mapCleared(MapEvent event) {
        handleMapEvent(event);
    }

    @Override
    protected String getUserCodeNamespace() {
        return ReplicatedMapService.lookupNamespace(nodeEngine, getDistributedObjectName());
    }
}
