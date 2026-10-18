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
import uk.org.hecaton.client.impl.protocol.codec.MapExecuteWithPredicateCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractMultiPartitionMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.partition.IPartition;
import uk.org.hecaton.internal.util.collection.PartitionIdSet;
import uk.org.hecaton.map.EntryProcessor;
import uk.org.hecaton.map.impl.MapEntries;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.operation.MapOperationProvider;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.query.PartitionPredicate;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.spi.impl.operationservice.OperationFactory;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static uk.org.hecaton.map.impl.MapService.SERVICE_NAME;

public class MapExecuteWithPredicateMessageTask
        extends AbstractMultiPartitionMessageTask<MapExecuteWithPredicateCodec.RequestParameters> {
    private Predicate predicate;

    public MapExecuteWithPredicateMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void beforeProcess() {
        predicate = serializationService.toObject(parameters.predicate);
    }

    @Override
    public PartitionIdSet getPartitions() {
        if (predicate instanceof PartitionPredicate partitionPredicate) {
            return clientEngine.getPartitionService().getPartitionIdSet(partitionPredicate.getPartitionKeys());
        }

        IPartition[] partitions = clientEngine.getPartitionService().getPartitions();
        ArrayList<Integer> partitionsList = new ArrayList<>(partitions.length);
        Arrays.stream(partitions).forEach(partition -> partitionsList.add(partition.getPartitionId()));
        return new PartitionIdSet(partitions.length, partitionsList);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        Predicate targetPredicate = predicate;
        if (predicate instanceof PartitionPredicate partitionPredicate) {
            targetPredicate = partitionPredicate.getTarget();
        }

        return createOperationFactory(targetPredicate);
    }

    private OperationFactory createOperationFactory(Predicate predicate) {
        MapOperationProvider operationProvider = getOperationProvider(parameters.name);
        EntryProcessor entryProcessor = serializationService.toObject(parameters.entryProcessor);
        return operationProvider.
                createPartitionWideEntryWithPredicateOperationFactory(parameters.name, entryProcessor, predicate);
    }

    private MapOperationProvider getOperationProvider(String mapName) {
        MapService mapService = getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapOperationProvider(mapName);
    }

    protected Object reduce(Map<Integer, Object> map) {
        List<Map.Entry<Data, Data>> dataMap = new ArrayList<>();
        MapService mapService = getService(MapService.SERVICE_NAME);
        for (Object o : map.values()) {
            if (o != null) {
                MapEntries mapEntries = (MapEntries) mapService.getMapServiceContext().toObject(o);
                mapEntries.putAllToList(dataMap);
            }
        }
        return dataMap;
    }

    @Override
    protected MapExecuteWithPredicateCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapExecuteWithPredicateCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapExecuteWithPredicateCodec.encodeResponse((List<Map.Entry<Data, Data>>) response);
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
        return SecurityInterceptorConstants.EXECUTE_ON_ENTRIES;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.entryProcessor, parameters.predicate};
    }

    @Override
    protected String getUserCodeNamespace() {
        return MapService.lookupNamespace(nodeEngine, getDistributedObjectName());
    }
}
