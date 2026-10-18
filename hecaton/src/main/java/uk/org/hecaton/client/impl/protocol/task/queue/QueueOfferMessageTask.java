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
import uk.org.hecaton.client.impl.protocol.codec.QueueOfferCodec;
import uk.org.hecaton.collection.impl.queue.operations.OfferOperation;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.QueuePermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.concurrent.TimeUnit;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link uk.org.hecaton.client.impl.protocol.codec.QueueOfferCodec#REQUEST_MESSAGE_TYPE}
 */
public class QueueOfferMessageTask
        extends AbstractQueueMessageTask<QueueOfferCodec.RequestParameters> {

    public QueueOfferMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new OfferOperation(parameters.name, parameters.timeoutMillis, parameters.value);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return QueueOfferCodec.encodeResponse((Boolean) response);
    }

    @Override
    protected QueueOfferCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return QueueOfferCodec.decodeRequest(clientMessage);
    }

    @Override
    public Object[] getParameters() {
        if (parameters.timeoutMillis > 0) {
            return new Object[]{parameters.value, parameters.timeoutMillis, TimeUnit.MILLISECONDS};
        }
        return new Object[]{parameters.value};
    }

    @Override
    public Permission getRequiredPermission() {
        return new QueuePermission(parameters.name, ActionConstants.ACTION_ADD);
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.OFFER;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }
}
