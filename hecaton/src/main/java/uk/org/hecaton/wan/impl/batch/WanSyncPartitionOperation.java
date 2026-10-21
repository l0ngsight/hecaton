/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.wan.impl.batch;

import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.map.impl.recordstore.expiry.ExpiryMetadata;
import uk.org.hecaton.map.impl.wan.WanMapAddOrUpdateEvent;
import uk.org.hecaton.map.impl.wan.WanMapEntryView;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.PartitionAwareOperation;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicy;
import uk.org.hecaton.wan.impl.WanReplicationService;
import uk.org.hecaton.wan.impl.merkletree.MerkleTreeUtil;

import java.util.Set;
import java.util.UUID;

import static uk.org.hecaton.map.impl.EntryViews.createWanEntryView;
import static uk.org.hecaton.internal.util.ToHeapDataConverter.toHeapData;

/**
 * Adds the entries of a map in a partition to a publisher's queue as sync events (WAN sync), or only
 * counts them. With leaves, only the entries in those Merkle tree leaves. Runs on the member that
 * owns the partition, which created it: it is never sent.
 */
class WanSyncPartitionOperation extends Operation implements PartitionAwareOperation {

    private final WanBatchPublisher publisher;
    private final String mapName;
    private final UUID syncId;
    private final Set<Integer> leaves;
    private final int leafLevel;
    private final boolean countOnly;
    private int entries;

    WanSyncPartitionOperation(WanBatchPublisher publisher, String mapName, UUID syncId, Set<Integer> leaves, int depth,
                              boolean countOnly) {
        this.publisher = publisher;
        this.mapName = mapName;
        this.syncId = syncId;
        this.leaves = leaves;
        this.leafLevel = depth - 1;
        this.countOnly = countOnly;
    }

    @Override
    public String getServiceName() {
        return WanReplicationService.SERVICE_NAME;
    }

    @Override
    public void run() {
        MapService mapService = getNodeEngine().getService(MapService.SERVICE_NAME);
        MapServiceContext context = mapService.getMapServiceContext();
        RecordStore<?> recordStore = context.getExistingRecordStore(getPartitionId(), mapName);
        if (recordStore == null) {
            return;
        }
        SplitBrainMergePolicy<?, ?, ?> mergePolicy = context.getMapContainer(mapName).getWanContext().getWanMergePolicy();
        recordStore.forEach((key, record) -> {
            if (leaves != null && !leaves.contains(MerkleTreeUtil.getLeafOrderForHash(key.hashCode(), leafLevel))) {
                return;
            }
            entries++;
            if (!countOnly) {
                Data value = toHeapData(context.toData(record.getValue()));
                ExpiryMetadata expiryMetadata = recordStore.getExpirySystem().getExpiryMetadata(key);
                WanMapEntryView<Object, Object> view = createWanEntryView(toHeapData(key), value, record, expiryMetadata,
                        getNodeEngine().getSerializationService());
                publisher.addSyncEvent(getPartitionId(), new WanMapAddOrUpdateEvent(mapName, mergePolicy, view), syncId);
            }
        }, false);
    }

    @Override
    public Object getResponse() {
        return entries;
    }
}
