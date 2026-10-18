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

package uk.org.hecaton.client.impl.protocol.task.multimap;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.task.AbstractAddListenerMessageTask;
import uk.org.hecaton.core.EntryAdapter;
import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.EntryEventType;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.map.MapEvent;
import uk.org.hecaton.map.impl.DataAwareEntryEvent;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.multimap.impl.MultiMapService;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MultiMapPermission;
import uk.org.hecaton.security.permission.UserCodeNamespacePermission;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.spi.impl.InternalCompletableFuture.newCompletedFuture;

public abstract class AbstractMultiMapAddEntryListenerMessageTask<P>
        extends AbstractAddListenerMessageTask<P> {

    protected AbstractMultiMapAddEntryListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<UUID> processInternal() {
        final MultiMapService service = getService(MultiMapService.SERVICE_NAME);
        EntryAdapter<?, ?> listener = new MultiMapListener();

        final String name = getDistributedObjectName();
        Data key = getKey();
        boolean includeValue = shouldIncludeValue();
        if (isLocalOnly()) {
            return newCompletedFuture(service.addLocalListener(name, listener, key, includeValue));
        }

        return service.addListenerAsync(name, listener, key, includeValue);
    }

    protected abstract boolean shouldIncludeValue();

    protected abstract boolean isLocalOnly();

    @Override
    public String getServiceName() {
        return MultiMapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MultiMapPermission(getDistributedObjectName(), ActionConstants.ACTION_LISTEN);
    }

    @Override
    public Permission getUserCodeNamespacePermission() {
        String namespace = MultiMapService.lookupNamespace(nodeEngine, getDistributedObjectName());
        return namespace != null ? new UserCodeNamespacePermission(namespace, ActionConstants.ACTION_USE) : null;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD_ENTRY_LISTENER;
    }

    public Data getKey() {
        return null;
    }

    private class MultiMapListener extends EntryAdapter<Object, Object> {

        @Override
        public void onEntryEvent(EntryEvent event) {
            if (endpoint.isAlive()) {
                if (!(event instanceof DataAwareEntryEvent)) {
                    throw new IllegalArgumentException("Expecting: DataAwareEntryEvent, Found: "
                            + event.getClass().getSimpleName());
                }
                DataAwareEntryEvent<?, ?> dataAwareEntryEvent = (DataAwareEntryEvent<?, ?>) event;
                Data key = dataAwareEntryEvent.getKeyData();
                Data value = dataAwareEntryEvent.getNewValueData();
                Data oldValue = dataAwareEntryEvent.getOldValueData();

                final EntryEventType type = event.getEventType();
                final UUID uuid = event.getMember().getUuid();

                sendClientMessage(key, encodeEvent(key, value, oldValue, type.getType(), uuid, 1));
            }
        }

        @Override
        public void onMapEvent(MapEvent event) {
            if (endpoint.isAlive()) {
                final EntryEventType type = event.getEventType();
                final UUID uuid = event.getMember().getUuid();
                sendClientMessage(null, encodeEvent(null,
                        null, null, type.getType(),
                        uuid, event.getNumberOfEntriesAffected()));
            }
        }
    }

    protected abstract ClientMessage encodeEvent(Data key, Data value, Data oldValue,
                                                 int type, UUID uuid, int numberOfEntriesAffected);

    @Override
    protected String getUserCodeNamespace() {
        return MapService.lookupNamespace(nodeEngine, getDistributedObjectName());
    }
}
