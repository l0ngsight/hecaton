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
import uk.org.hecaton.client.impl.protocol.codec.MapSubmitToKeyCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.map.EntryProcessor;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.operation.MapOperation;
import uk.org.hecaton.map.impl.operation.MapOperationProvider;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;


public class MapSubmitToKeyMessageTask
        extends AbstractMapPartitionMessageTask<MapSubmitToKeyCodec.RequestParameters> {

    public MapSubmitToKeyMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
        setNamespaceAware();
    }

    @Override
    protected Operation prepareOperation() {
        EntryProcessor processor = serializationService.toObject(parameters.entryProcessor);
        MapOperationProvider operationProvider = getMapOperationProvider(parameters.name);
        MapOperation op = operationProvider.createEntryOperation(parameters.name, parameters.key, processor);
        op.setThreadId(parameters.threadId);
        return op;
    }

    @Override
    protected MapSubmitToKeyCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapSubmitToKeyCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapSubmitToKeyCodec.encodeResponse(serializationService.toData(response));
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_PUT, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
    return SecurityInterceptorConstants.SUBMIT_TO_KEY;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key, parameters.entryProcessor};
    }
}

