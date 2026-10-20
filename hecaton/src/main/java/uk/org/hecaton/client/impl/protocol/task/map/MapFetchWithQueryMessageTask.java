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
import uk.org.hecaton.client.impl.protocol.codec.MapFetchWithQueryCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.iteration.IterationPointer;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.util.IterationType;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.operation.MapOperationProvider;
import uk.org.hecaton.map.impl.query.Query;
import uk.org.hecaton.map.impl.query.QueryResult;
import uk.org.hecaton.map.impl.query.QueryResultRow;
import uk.org.hecaton.map.impl.query.ResultSegment;
import uk.org.hecaton.projection.Projection;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.ArrayList;
import java.util.List;

import static uk.org.hecaton.internal.iteration.IterationPointer.decodePointers;
import static uk.org.hecaton.internal.iteration.IterationPointer.encodePointers;

/**
 * Fetches by query a batch of items from a single partition ID for a map. The query is run by the query engine which means it
 * supports projections and filtering.
 *
 * @see uk.org.hecaton.map.impl.proxy.MapProxyImpl#iterator(int, int, Projection, Predicate)
 * @since 3.9
 */
public class MapFetchWithQueryMessageTask
        extends AbstractMapPartitionMessageTask<MapFetchWithQueryCodec.RequestParameters> {
    public MapFetchWithQueryMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
        setNamespaceAware();
    }

    @Override
    protected Operation prepareOperation() {
        MapOperationProvider operationProvider = getMapOperationProvider(parameters.name);
        Projection<?, ?> projection = nodeEngine.getSerializationService().toObject(parameters.projection);
        Predicate predicate = nodeEngine.getSerializationService().toObject(parameters.predicate);
        Query query = Query.of()
                           .mapName(parameters.name)
                           .iterationType(IterationType.VALUE)
                           .predicate(predicate)
                           .projection(projection)
                           .build();
        IterationPointer[] pointers = decodePointers(parameters.iterationPointers);
        return operationProvider.createFetchWithQueryOperation(parameters.name, pointers, parameters.batch, query);
    }

    @Override
    protected MapFetchWithQueryCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapFetchWithQueryCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        ResultSegment resp = (ResultSegment) response;
        QueryResult queryResult = (QueryResult) resp.getResult();

        List<Data> serialized = new ArrayList<>(queryResult.size());
        for (QueryResultRow row : queryResult) {
            serialized.add(row.getValue());
        }
        IterationPointer[] pointers = resp.getPointers();
        return MapFetchWithQueryCodec.encodeResponse(serialized, encodePointers(pointers));
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
        return SecurityInterceptorConstants.ITERATOR_FETCH_WITH_QUERY;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.batch, getPartitionId(), parameters.projection, parameters.predicate};
    }
}
