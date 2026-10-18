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
import uk.org.hecaton.client.impl.protocol.codec.MapFetchKeysCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.iteration.IterationPointer;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.iterator.MapKeysWithCursor;
import uk.org.hecaton.map.impl.operation.MapOperationProvider;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.Collections;

import static uk.org.hecaton.internal.iteration.IterationPointer.decodePointers;
import static uk.org.hecaton.internal.iteration.IterationPointer.encodePointers;

public class MapFetchKeysMessageTask extends AbstractMapPartitionMessageTask<MapFetchKeysCodec.RequestParameters> {
    public MapFetchKeysMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        MapOperationProvider operationProvider = getMapOperationProvider(parameters.name);
        IterationPointer[] pointers = decodePointers(parameters.iterationPointers);
        return operationProvider.createFetchKeysOperation(parameters.name, pointers, parameters.batch);
    }

    @Override
    protected MapFetchKeysCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapFetchKeysCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        if (response == null) {
            return MapFetchKeysCodec.encodeResponse(Collections.emptyList(), Collections.emptyList());
        }
        MapKeysWithCursor mapKeysWithCursor = (MapKeysWithCursor) response;
        IterationPointer[] pointers = mapKeysWithCursor.getIterationPointers();
        return MapFetchKeysCodec.encodeResponse(
                encodePointers(pointers), mapKeysWithCursor.getBatch());
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.ITERATOR_FETCH_KEYS;
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
