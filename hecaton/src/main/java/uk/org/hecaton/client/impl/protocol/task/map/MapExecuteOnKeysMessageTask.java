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
import uk.org.hecaton.client.impl.protocol.codec.MapExecuteOnKeysCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractMultiPartitionMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.map.EntryProcessor;
import uk.org.hecaton.map.impl.MapEntries;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.map.impl.operation.MapOperationProvider;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.spi.impl.operationservice.OperationFactory;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.internal.util.collection.PartitionIdSet;

import java.security.Permission;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
public class MapExecuteOnKeysMessageTask
        extends AbstractMultiPartitionMessageTask<MapExecuteOnKeysCodec.RequestParameters> {

    public MapExecuteOnKeysMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected OperationFactory createOperationFactory() {
        EntryProcessor processor = serializationService.toObject(parameters.entryProcessor);
        MapOperationProvider operationProvider = getMapOperationProvider(parameters.name);
        return operationProvider.createMultipleEntryOperationFactory(parameters.name,
                new HashSet<>(parameters.keys), processor);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        List<Map.Entry<Data, Data>> entries = new ArrayList<>();

        MapService mapService = getService(MapService.SERVICE_NAME);
        for (Object o : map.values()) {
            if (o != null) {
                MapEntries mapEntries = (MapEntries) mapService.getMapServiceContext().toObject(o);
                mapEntries.putAllToList(entries);
            }
        }
        return entries;
    }

    @Override
    public PartitionIdSet getPartitions() {
        IPartitionService partitionService = nodeEngine.getPartitionService();
        int partitions = partitionService.getPartitionCount();
        PartitionIdSet partitionIds = new PartitionIdSet(partitions);
        Iterator<Data> iterator = parameters.keys.iterator();
        int addedPartitions = 0;
        while (iterator.hasNext() && addedPartitions < partitions) {
            Data key = iterator.next();
            if (partitionIds.add(partitionService.getPartitionId(key))) {
                addedPartitions++;
            }
        }
        return partitionIds;
    }

    @Override
    protected MapExecuteOnKeysCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapExecuteOnKeysCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapExecuteOnKeysCodec.encodeResponse((List<Map.Entry<Data, Data>>) response);
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
        return SecurityInterceptorConstants.EXECUTE_ON_KEYS;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.keys, parameters.entryProcessor};
    }

    @Override
    protected String getUserCodeNamespace() {
        return MapService.lookupNamespace(nodeEngine, getDistributedObjectName());
    }
}
