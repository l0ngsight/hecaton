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
import uk.org.hecaton.client.impl.protocol.codec.MapPutTransientCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.map.impl.operation.MapOperation;
import uk.org.hecaton.map.impl.operation.MapOperationProvider;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.map.impl.record.Record.UNSET;

public class MapPutTransientMessageTask
        extends AbstractMapPutMessageTask<MapPutTransientCodec.RequestParameters> {

    public MapPutTransientMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        MapOperation op = newOperation(parameters.name, parameters.key,
                parameters.value, parameters.ttl);
        op.setThreadId(parameters.threadId);
        return op;
    }

    private MapOperation newOperation(String name, Data keyData, Data valueData, long ttl) {
        MapOperationProvider operationProvider = getMapOperationProvider(name);
        return operationProvider.createPutTransientOperation(name, keyData, valueData, ttl, UNSET);
    }

    @Override
    protected MapPutTransientCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapPutTransientCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapPutTransientCodec.encodeResponse();
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.PUT_TRANSIENT;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key, parameters.value, parameters.ttl, TimeUnit.MILLISECONDS};
    }
}
