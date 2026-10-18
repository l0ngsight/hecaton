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

package uk.org.hecaton.client.impl.protocol.task.queue;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.QueueAddListenerCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractAddListenerMessageTask;
import uk.org.hecaton.collection.ItemEvent;
import uk.org.hecaton.collection.ItemListener;
import uk.org.hecaton.collection.impl.common.DataAwareItemEvent;
import uk.org.hecaton.collection.impl.queue.QueueService;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.QueuePermission;

import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.spi.impl.InternalCompletableFuture.newCompletedFuture;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link uk.org.hecaton.client.impl.protocol.codec.QueueAddListenerCodec#REQUEST_MESSAGE_TYPE}
 */
public class QueueAddListenerMessageTask
        extends AbstractAddListenerMessageTask<QueueAddListenerCodec.RequestParameters> {

    public QueueAddListenerMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<UUID> processInternal() {
        final QueueService service = getService(QueueService.SERVICE_NAME);
        final Data partitionKey = serializationService.toData(parameters.name);
        ItemListener listener = new ItemListener() {
            @Override
            public void itemAdded(ItemEvent item) {
                send(item);
            }

            @Override
            public void itemRemoved(ItemEvent item) {
                send(item);
            }

            private void send(ItemEvent event) {
                if (endpoint.isAlive()) {

                    if (!(event instanceof DataAwareItemEvent)) {
                        throw new IllegalArgumentException(
                                "Expecting: DataAwareItemEvent, Found: " + event.getClass().getSimpleName());
                    }

                    DataAwareItemEvent dataAwareItemEvent = (DataAwareItemEvent) event;
                    Data item = dataAwareItemEvent.getItemData();
                    ClientMessage clientMessage = QueueAddListenerCodec.encodeItemEvent(item,
                            event.getMember().getUuid(), event.getEventType().getType());
                    sendClientMessage(partitionKey, clientMessage);
                }
            }
        };

        if (parameters.localOnly) {
            return newCompletedFuture(service.addLocalItemListener(parameters.name, listener, parameters.includeValue));
        }

        return service.addItemListenerAsync(parameters.name, listener, parameters.includeValue);
    }

    @Override
    protected QueueAddListenerCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return QueueAddListenerCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return QueueAddListenerCodec.encodeResponse((UUID) response);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{null, parameters.includeValue};
    }

    @Override
    public Permission getRequiredPermission() {
        return new QueuePermission(parameters.name, ActionConstants.ACTION_LISTEN);
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD_ITEM_LISTENER;
    }

    @Override
    public String getServiceName() {
        return QueueService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    protected String getUserCodeNamespace() {
        return QueueService.lookupNamespace(nodeEngine, getDistributedObjectName());
    }
}
