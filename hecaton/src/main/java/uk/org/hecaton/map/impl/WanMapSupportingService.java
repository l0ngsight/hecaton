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

package uk.org.hecaton.map.impl;

import uk.org.hecaton.internal.util.HecatonFeature;
import uk.org.hecaton.config.WanAcknowledgeType;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.services.WanSupportingService;
import uk.org.hecaton.map.impl.operation.MapOperation;
import uk.org.hecaton.map.impl.operation.MapOperationProvider;
import uk.org.hecaton.map.impl.wan.WanMapAddOrUpdateEvent;
import uk.org.hecaton.map.impl.wan.WanMapRemoveEvent;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import uk.org.hecaton.wan.WanEventCounters;
import uk.org.hecaton.wan.impl.InternalWanEvent;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;

import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static uk.org.hecaton.map.impl.MapService.SERVICE_NAME;
import static uk.org.hecaton.spi.impl.merge.MergingValueFactory.createMergingEntry;

class WanMapSupportingService implements WanSupportingService {
    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;
    private final WanEventCounters wanEventTypeCounters;

    WanMapSupportingService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.wanEventTypeCounters = nodeEngine.getWanReplicationService()
                .getReceivedEventCounters(MapService.SERVICE_NAME);
    }

    @Override
    public void onReplicationEvent(InternalWanEvent event, WanAcknowledgeType acknowledgeType) {
        if (event instanceof WanMapAddOrUpdateEvent updateEvent) {
            handleAddOrUpdate(updateEvent);
        } else if (event instanceof WanMapRemoveEvent removeEvent) {
            handleRemove(removeEvent);
        }
    }

    @Override
    public CompletionStage<Void> onSyncBatch(Collection<InternalWanEvent> batch, WanAcknowledgeType acknowledgeType) {
        // code should never reach here
        throw new UnsupportedOperationException(HecatonFeature.WAN_SYNC.notAvailableMessage());
    }

    @Override
    public void onWanConfigChange() {
        // no-op
    }

    private void handleRemove(WanMapRemoveEvent replicationRemove) {
        String mapName = replicationRemove.getObjectName();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);
        MapOperation operation = operationProvider.createDeleteOperation(replicationRemove.getObjectName(),
                replicationRemove.getKey(), true);

        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(replicationRemove.getKey());
            Future future = nodeEngine.getOperationService()
                    .invokeOnPartition(SERVICE_NAME, operation, partitionId);
            future.get();
            wanEventTypeCounters.incrementRemove(mapName);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private void handleAddOrUpdate(WanMapAddOrUpdateEvent replicationUpdate) {
        SplitBrainMergePolicy mergePolicy = replicationUpdate.getMergePolicy();
        String mapName = replicationUpdate.getObjectName();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);

        SerializationService serializationService = nodeEngine.getSerializationService();
        MapMergeTypes<Object, Object> mergingEntry = createMergingEntry(serializationService, replicationUpdate.getEntryView());
        //noinspection unchecked
        MapOperation operation = operationProvider.createMergeOperation(mapName, mergingEntry,
                (SplitBrainMergePolicy<Object, MapMergeTypes<Object, Object>, Object>) mergePolicy, true);

        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(replicationUpdate.getEntryView().getKey());
            Future future = nodeEngine.getOperationService()
                    .invokeOnPartition(SERVICE_NAME, operation, partitionId);
            future.get();
            wanEventTypeCounters.incrementUpdate(mapName);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }
}
