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
import uk.org.hecaton.client.impl.protocol.codec.MapPutAllWithMetadataCodec;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.util.Timer;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.SimpleEntryView;
import uk.org.hecaton.map.impl.operation.MergeOperation;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.MapPermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.merge.PassThroughMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.MapMergeTypes;

import java.security.Permission;
import java.util.ArrayList;
import java.util.List;

import static uk.org.hecaton.spi.impl.merge.MergingValueFactory.createMergingEntry;

public class MapPutAllWithMetadataMessageTask
        extends AbstractMapPartitionMessageTask<MapPutAllWithMetadataCodec.RequestParameters> {

    private volatile long startTimeNanos;

    public MapPutAllWithMetadataMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        SerializationService ss = nodeEngine.getSerializationService();

        List<MapMergeTypes<Object, Object>> mergingEntries = new ArrayList<>();
        for (SimpleEntryView<Data, Data> entry : parameters.entries) {
            mergingEntries.add(createMergingEntry(ss, entry));
        }

        SplitBrainMergePolicy mergePolicy = nodeEngine.getSplitBrainMergePolicyProvider()
                                                      .getMergePolicy(PassThroughMergePolicy.class.getName());

        return new MergeOperation(parameters.name, mergingEntries, mergePolicy, true);
    }

    @Override
    protected MapPutAllWithMetadataCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapPutAllWithMetadataCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapPutAllWithMetadataCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    protected void beforeProcess() {
        startTimeNanos = Timer.nanos();
    }

    @Override
    protected Object processResponseBeforeSending(Object response) {
        MapService mapService = getService(MapService.SERVICE_NAME);
        MapContainer mapContainer = mapService.getMapServiceContext().getMapContainer(parameters.name);
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            mapService.getMapServiceContext().getLocalMapStatsProvider().getLocalMapStatsImpl(parameters.name)
                    .incrementPutLatencyNanos(parameters.entries.size(),
                            Timer.nanosElapsed(startTimeNanos));
        }
        return response;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(parameters.name, ActionConstants.ACTION_PUT);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "putAllWithMetadata";
    }

    @Override
    public Object[] getParameters() {
        List<SimpleEntryView<Data, Data>> entries = new ArrayList<>(parameters.entries);
        return new Object[]{entries};
    }
}
