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
import uk.org.hecaton.client.impl.protocol.codec.RingbufferReadManyCodec;
import uk.org.hecaton.core.IFunction;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.ringbuffer.impl.ReadResultSetImpl;
import uk.org.hecaton.ringbuffer.impl.operations.ReadManyOperation;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.RingBufferPermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.ArrayList;
import java.util.List;

/**
 * Client Protocol Task for handling messages with type ID:
 * {@link uk.org.hecaton.client.impl.protocol.codec.RingbufferReadManyCodec#REQUEST_MESSAGE_TYPE}
 */
public class RingbufferReadManyMessageTask
        extends AbstractRingbufferMessageTask<RingbufferReadManyCodec.RequestParameters> {

    public RingbufferReadManyMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
        setNamespaceAware();
    }

    @Override
    protected Operation prepareOperation() {
        IFunction<?, Boolean> function = serializationService.toObject(parameters.filter);
        return new ReadManyOperation(
                parameters.name,
                parameters.startSequence,
                parameters.minCount,
                parameters.maxCount,
                function);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        // we are not deserializing the whole content, only the enclosing portable. The actual items remain un
        final ReadResultSetImpl resultSet = nodeEngine.getSerializationService().toObject(response);
        final List<Data> items = new ArrayList<>(resultSet.size());
        final long[] seqs = new long[resultSet.size()];
        final Data[] dataItems = resultSet.getDataItems();

        for (int k = 0; k < resultSet.size(); k++) {
            items.add(dataItems[k]);
            seqs[k] = resultSet.getSequence(k);
        }

        return RingbufferReadManyCodec.encodeResponse(resultSet.readCount(), items, seqs, resultSet.getNextSequenceToReadFrom());
    }

    @Override
    protected RingbufferReadManyCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return RingbufferReadManyCodec.decodeRequest(clientMessage);
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.READ_MANY;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Permission getRequiredPermission() {
        return new RingBufferPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public Object[] getParameters() {

        return new Object[]{parameters.startSequence, parameters.minCount, parameters.maxCount, null};
    }
}
