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
import uk.org.hecaton.client.impl.protocol.codec.QueueAddAllCodec;
import uk.org.hecaton.collection.impl.queue.operations.AddAllOperation;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.QueuePermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link uk.org.hecaton.client.impl.protocol.codec.QueueAddListenerCodec#REQUEST_MESSAGE_TYPE}
 */
public class QueueAddAllMessageTask
        extends AbstractQueueMessageTask<QueueAddAllCodec.RequestParameters> {

    public QueueAddAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new AddAllOperation(parameters.name, parameters.dataList);
    }

    @Override
    protected QueueAddAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return QueueAddAllCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        final boolean result = response != null && ((Boolean) response);
        return QueueAddAllCodec.encodeResponse(result);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.dataList};
    }

    @Override
    public Permission getRequiredPermission() {
        return new QueuePermission(parameters.name, ActionConstants.ACTION_ADD);
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD_ALL;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

}
