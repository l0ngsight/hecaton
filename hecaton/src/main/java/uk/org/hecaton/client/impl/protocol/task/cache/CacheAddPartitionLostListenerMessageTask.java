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

package uk.org.hecaton.client.impl.protocol.task.cache;

import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.cache.impl.event.CachePartitionLostEventFilter;
import uk.org.hecaton.cache.impl.event.CachePartitionLostListener;
import uk.org.hecaton.cache.impl.event.InternalCachePartitionLostListenerAdapter;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CacheAddPartitionLostListenerCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractAddListenerMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.CachePermission;
import uk.org.hecaton.security.permission.UserCodeNamespacePermission;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.spi.impl.eventservice.EventFilter;
import uk.org.hecaton.spi.impl.eventservice.EventRegistration;
import uk.org.hecaton.spi.impl.eventservice.EventService;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static uk.org.hecaton.spi.impl.InternalCompletableFuture.newCompletedFuture;

public class CacheAddPartitionLostListenerMessageTask
        extends AbstractAddListenerMessageTask<CacheAddPartitionLostListenerCodec.RequestParameters> {

    public CacheAddPartitionLostListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<UUID> processInternal() {
        CachePartitionLostListener listener = event -> {
            if (endpoint.isAlive()) {
                ClientMessage eventMessage = CacheAddPartitionLostListenerCodec
                        .encodeCachePartitionLostEvent(event.getPartitionId(), event.getMember().getUuid());
                sendClientMessage(null, eventMessage);
            }
        };

        InternalCachePartitionLostListenerAdapter listenerAdapter =
                new InternalCachePartitionLostListenerAdapter(listener);
        EventFilter filter = new CachePartitionLostEventFilter();
        CacheService service = getService(getServiceName());
        EventService eventService = service.getNodeEngine().getEventService();
        if (parameters.localOnly) {
            return newCompletedFuture(
                    eventService.registerLocalListener(getServiceName(), parameters.name, filter, listenerAdapter)
                                .getId());
        }

        return eventService.registerListenerAsync(getServiceName(), parameters.name, filter, listenerAdapter)
                           .thenApplyAsync(EventRegistration::getId, CALLER_RUNS);
    }

    @Override
    protected CacheAddPartitionLostListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CacheAddPartitionLostListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CacheAddPartitionLostListenerCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD_PARTITION_LOST_LISTENER;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return new CachePermission(getDistributedObjectName(), ActionConstants.ACTION_LISTEN);
    }

    @Override
    public Permission getUserCodeNamespacePermission() {
        String namespace = getUserCodeNamespace();
        return namespace != null ? new UserCodeNamespacePermission(namespace, ActionConstants.ACTION_USE) : null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    protected String getUserCodeNamespace() {
        return CacheService.lookupNamespace(nodeEngine, parameters.name);
    }
}
