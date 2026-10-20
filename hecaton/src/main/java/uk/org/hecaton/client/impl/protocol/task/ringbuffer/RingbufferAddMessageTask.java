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

package uk.org.hecaton.client.impl.protocol.task.ringbuffer;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.RingbufferAddCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.ringbuffer.OverflowPolicy;
import uk.org.hecaton.ringbuffer.impl.operations.AddOperation;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.RingBufferPermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link uk.org.hecaton.client.impl.protocol.codec.RingbufferAddCodec#REQUEST_MESSAGE_TYPE}
 */
public class RingbufferAddMessageTask
        extends AbstractRingbufferMessageTask<RingbufferAddCodec.RequestParameters> {

    public RingbufferAddMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new AddOperation(parameters.name, parameters.value, OverflowPolicy.getById(parameters.overflowPolicy));
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return RingbufferAddCodec.encodeResponse((Long) response);
    }

    @Override
    protected RingbufferAddCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return RingbufferAddCodec.decodeRequest(clientMessage);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.value, OverflowPolicy.getById(parameters.overflowPolicy)};
    }

    @Override
    public Permission getRequiredPermission() {
        return new RingBufferPermission(parameters.name, ActionConstants.ACTION_PUT);
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ADD;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }
}
