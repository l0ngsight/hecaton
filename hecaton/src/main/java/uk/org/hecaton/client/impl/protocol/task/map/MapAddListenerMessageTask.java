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
import uk.org.hecaton.client.impl.protocol.codec.ContinuousQueryAddListenerCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractAddListenerMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.map.IMapEvent;
import uk.org.hecaton.map.impl.ListenerAdapter;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.querycache.event.BatchEventData;
import uk.org.hecaton.map.impl.querycache.event.BatchIMapEvent;
import uk.org.hecaton.map.impl.querycache.event.QueryCacheEventData;
import uk.org.hecaton.map.impl.querycache.event.SingleIMapEvent;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.spi.impl.eventservice.impl.TrueEventFilter;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.spi.impl.InternalCompletableFuture.newCompletedFuture;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link uk.org.hecaton.client.impl.protocol.codec.MapAddEntryListenerCodec#REQUEST_MESSAGE_TYPE}
 */
public class MapAddListenerMessageTask
        extends AbstractAddListenerMessageTask<ContinuousQueryAddListenerCodec.RequestParameters>
        implements ListenerAdapter<IMapEvent> {

    public MapAddListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<UUID> processInternal() {
        MapService mapService = getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        if (parameters.localOnly) {
            return newCompletedFuture(mapServiceContext.addLocalListenerAdapter(this, parameters.listenerName));
        }

        return mapServiceContext.addListenerAdapterAsync(this, TrueEventFilter.INSTANCE, parameters.listenerName);
    }

    @Override
    public void onEvent(IMapEvent iMapEvent) {
        if (!endpoint.isAlive()) {
            return;
        }
        ClientMessage eventData = getEventData(iMapEvent);
        sendClientMessage(eventData);
    }

    private ClientMessage getEventData(IMapEvent iMapEvent) {
        if (iMapEvent instanceof SingleIMapEvent event) {
            QueryCacheEventData eventData = event.getEventData();
            ClientMessage clientMessage = ContinuousQueryAddListenerCodec.encodeQueryCacheSingleEvent(eventData);
            int partitionId = eventData.getPartitionId();
            clientMessage.setPartitionId(partitionId);
            return clientMessage;
        }

        if (iMapEvent instanceof BatchIMapEvent batchIMapEvent) {
            BatchEventData batchEventData = batchIMapEvent.getBatchEventData();
            int partitionId = batchEventData.getPartitionId();
            ClientMessage clientMessage =
                    ContinuousQueryAddListenerCodec.encodeQueryCacheBatchEvent(batchEventData.getEvents(),
                            batchEventData.getSource(), partitionId);
            clientMessage.setPartitionId(partitionId);
            return clientMessage;
        }

        throw new IllegalArgumentException("Unexpected event type found = [" + iMapEvent + "]");
    }

    @Override
    protected ContinuousQueryAddListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ContinuousQueryAddListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ContinuousQueryAddListenerCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.listenerName;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD_ENTRY_LISTENER;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    protected String getUserCodeNamespace() {
        return MapService.lookupNamespace(nodeEngine, getDistributedObjectName());
    }
}
