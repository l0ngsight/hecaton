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
import uk.org.hecaton.client.impl.protocol.codec.RingbufferAddAllCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.ringbuffer.OverflowPolicy;
import uk.org.hecaton.ringbuffer.impl.operations.AddAllOperation;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.RingBufferPermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.List;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link uk.org.hecaton.client.impl.protocol.codec.RingbufferAddAllCodec#REQUEST_MESSAGE_TYPE}
 */
public class RingbufferAddAllMessageTask
        extends AbstractRingbufferMessageTask<RingbufferAddAllCodec.RequestParameters> {

    public RingbufferAddAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new AddAllOperation(parameters.name, items(), OverflowPolicy.getById(parameters.overflowPolicy));
    }

    private Data[] items() {
        List<Data> valueList = parameters.valueList;
        Data[] array = new Data[valueList.size()];
        return valueList.toArray(array);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return RingbufferAddAllCodec.encodeResponse((Long) response);
    }

    @Override
    protected RingbufferAddAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return RingbufferAddAllCodec.decodeRequest(clientMessage);
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.valueList, OverflowPolicy.getById(parameters.overflowPolicy)};
    }

    @Override
    public Permission getRequiredPermission() {
        return new RingBufferPermission(parameters.name, ActionConstants.ACTION_PUT);
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
