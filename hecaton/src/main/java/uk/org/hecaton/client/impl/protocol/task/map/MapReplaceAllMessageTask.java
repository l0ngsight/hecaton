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
import uk.org.hecaton.client.impl.protocol.codec.MapReplaceAllCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.map.impl.MapEntryReplacingEntryProcessor;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.operation.MapOperationProvider;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationFactory;

import java.security.Permission;
import java.util.function.BiFunction;

public class MapReplaceAllMessageTask
        extends AbstractMapPartitionMessageTask<MapReplaceAllCodec.RequestParameters> {

    private BiFunction function;

    public MapReplaceAllMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
        setNamespaceAware();
    }

    @Override
    protected Operation prepareOperation() {
        OperationFactory operationFactory = createOperationFactory();
        return operationFactory.createOperation();
    }

    protected OperationFactory createOperationFactory() {
        MapOperationProvider operationProvider = getMapOperationProvider(parameters.name);
        return operationProvider.createPartitionWideEntryWithPredicateOperationFactory(parameters.name,
           new MapEntryReplacingEntryProcessor(function), Predicates.alwaysTrue());
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    protected MapReplaceAllCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        MapReplaceAllCodec.RequestParameters parameters =  MapReplaceAllCodec.decodeRequest(clientMessage);
        function = serializationService.toObject(parameters.function);
        return parameters;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapReplaceAllCodec.encodeResponse();
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(getDistributedObjectName(), ActionConstants.ACTION_PUT);
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.REPLACE_ALL;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters};
    }
}
