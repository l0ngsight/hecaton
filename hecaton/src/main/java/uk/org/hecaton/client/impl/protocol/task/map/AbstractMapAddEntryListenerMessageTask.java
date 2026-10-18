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

package uk.org.hecaton.client.impl.protocol.task.map;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.task.AbstractAddListenerMessageTask;
import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.EntryEventType;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.map.MapEvent;
import uk.org.hecaton.map.impl.DataAwareEntryEvent;
import uk.org.hecaton.map.impl.MapListenerAdapter;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.security.permission.UserCodeNamespacePermission;
import uk.org.hecaton.spi.impl.eventservice.EventFilter;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.spi.impl.InternalCompletableFuture.newCompletedFuture;

public abstract class AbstractMapAddEntryListenerMessageTask<Parameter>
        extends AbstractAddListenerMessageTask<Parameter> {

    public AbstractMapAddEntryListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<UUID> processInternal() {
        final MapService mapService = getService(MapService.SERVICE_NAME);

        Object listener = newMapListener();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        String name = getDistributedObjectName();
        EventFilter eventFilter = getEventFilter();
        if (isLocalOnly()) {
            return newCompletedFuture(mapServiceContext.addLocalEventListener(listener, eventFilter, name));
        }

        return mapServiceContext.addEventListenerAsync(listener, eventFilter, name);
    }

    protected Object newMapListener() {
        return new ClientMapListener();
    }

    protected abstract EventFilter getEventFilter();

    protected abstract boolean isLocalOnly();

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public String getMethodName() {
        return "addEntryListener";
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(getDistributedObjectName(), ActionConstants.ACTION_LISTEN);
    }

    @Override
    public Permission getUserCodeNamespacePermission() {
        String namespace = getUserCodeNamespace();
        return namespace != null ? new UserCodeNamespacePermission(namespace, ActionConstants.ACTION_USE) : null;
    }

    private class ClientMapListener extends MapListenerAdapter<Object, Object> {

        @Override
        public void onEntryEvent(EntryEvent<Object, Object> event) {
            if (!endpoint.isAlive()) {
                return;
            }
            if (!(event instanceof DataAwareEntryEvent)) {
                throw new IllegalArgumentException(
                        "Expecting: DataAwareEntryEvent, Found: " + event.getClass().getSimpleName());
            }
            DataAwareEntryEvent dataAwareEntryEvent = (DataAwareEntryEvent) event;
            Data keyData = dataAwareEntryEvent.getKeyData();
            Data newValueData = dataAwareEntryEvent.getNewValueData();
            Data oldValueData = dataAwareEntryEvent.getOldValueData();
            Data meringValueData = dataAwareEntryEvent.getMergingValueData();
            sendClientMessage(keyData, encodeEvent(keyData
                    , newValueData, oldValueData, meringValueData, event.getEventType().getType(),
                    event.getMember().getUuid(), 1));

        }

        @Override
        public void onMapEvent(MapEvent event) {
            if (!endpoint.isAlive()) {
                return;
            }
            EntryEventType type = event.getEventType();
            UUID uuid = event.getMember().getUuid();
            int numberOfEntriesAffected = event.getNumberOfEntriesAffected();
            sendClientMessage(null, encodeEvent(null,
                    null, null, null, type.getType(), uuid, numberOfEntriesAffected));
        }
    }

    protected abstract ClientMessage encodeEvent(Data keyData, Data newValueData,
                                                 Data oldValueData, Data meringValueData,
                                                 int type, UUID uuid, int numberOfEntriesAffected);

    @Override
    protected String getUserCodeNamespace() {
        return MapService.lookupNamespace(nodeEngine, getDistributedObjectName());
    }
}
