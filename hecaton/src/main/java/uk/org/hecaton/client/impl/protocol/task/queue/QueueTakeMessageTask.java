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
import uk.org.hecaton.client.impl.protocol.codec.QueueTakeCodec;
import uk.org.hecaton.collection.impl.queue.operations.PollOperation;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.QueuePermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link uk.org.hecaton.client.impl.protocol.codec.QueueTakeCodec#REQUEST_MESSAGE_TYPE}
 */
public class QueueTakeMessageTask
        extends AbstractQueueMessageTask<String> {

    public QueueTakeMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new PollOperation(parameters, -1);
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return QueueTakeCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return QueueTakeCodec.encodeResponse((Data) response);
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public Permission getRequiredPermission() {
        return new QueuePermission(parameters, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.TAKE;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }
}
