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

package uk.org.hecaton.replicatedmap.impl.operation;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.impl.SerializationUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapEventPublishingService;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapService;
import uk.org.hecaton.replicatedmap.impl.record.RecordMigrationInfo;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedMapEntryViewHolder;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedRecord;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedRecordStore;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static uk.org.hecaton.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;

/**
 * Puts a list of {@link uk.org.hecaton.replicatedmap.impl.record.ReplicatedMapEntryView}s to the ReplicatedMap with given name.
 * All entryViews have to belong to the same partition. The version will be used in
 * {@link ReplicatedRecordStore#putRecords(Collection, long)}
 */
public class PutAllWithMetadataOperation extends AbstractNamedSerializableOperation implements PartitionAwareOperation,
        MutatingOperation {

    private String name;
    private final List<ReplicatedMapEntryViewHolder> entryViewHolders;
    public PutAllWithMetadataOperation() {
        this.entryViewHolders = new ArrayList<>();
    }

    public PutAllWithMetadataOperation(String name, List<ReplicatedMapEntryViewHolder> entryViewHolders, int partitionId) {
        this.name = name;
        this.entryViewHolders = entryViewHolders;
        this.setPartitionId(partitionId);
    }

    @Override
    public void run() throws Exception {
        ReplicatedMapService service = getService();
        ReplicatedRecordStore store = service.getReplicatedRecordStore(name, true, getPartitionId());
        int partitionId = getPartitionId();
        IPartitionService partitionService = getNodeEngine().getPartitionService();
        ReplicatedMapEventPublishingService eventPublishingService = service.getEventPublishingService();
        for (ReplicatedMapEntryViewHolder entryViewHolder : entryViewHolders) {
            Data key = entryViewHolder.getKey();
            if (partitionId != partitionService.getPartitionId(key)) {
                continue;
            }
            Data value = entryViewHolder.getValue();
            RecordMigrationInfo record = new RecordMigrationInfo(entryViewHolder.getKey(),
                    value, entryViewHolder.getTtlMillis());
            record.setCreationTime(entryViewHolder.getCreationTime());
            record.setHits(entryViewHolder.getHits());
            record.setLastAccessTime(entryViewHolder.getLastAccessTime());
            record.setLastUpdateTime(entryViewHolder.getLastUpdateTime());
            ReplicatedRecord oldRecord = store.putRecord(record);
            Object putResult = oldRecord != null ? oldRecord.getValue() : null;
            Data oldValue = getNodeEngine().toData(putResult);
            eventPublishingService.fireEntryListenerEvent(key, oldValue, value, name, getCallerAddress());
            VersionResponsePair response = new VersionResponsePair(putResult, store.getVersion());
            publishReplicationMessage(key, value, response, entryViewHolder.getTtlMillis());
        }
    }

    private void publishReplicationMessage(Data key, Data value, VersionResponsePair response, long ttl) {
        OperationService operationService = getNodeEngine().getOperationService();
        Collection<Member> members = getNodeEngine().getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        for (Member member : members) {
            Address address = member.getAddress();
            if (address.equals(getNodeEngine().getThisAddress())) {
                continue;
            }
            Operation op = new ReplicateUpdateOperation(name, key, value, ttl, response, false, getCallerAddress())
                    .setPartitionId(getPartitionId())
                    .setValidateTarget(false);
            operationService.invokeOnTarget(getServiceName(), op, address);
        }
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.PUT_ALL_WITH_METADATA;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        SerializationUtil.writeList(entryViewHolders, out);
        out.writeInt(getPartitionId());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readString();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            ReplicatedMapEntryViewHolder entryViewHolder = in.readObject(ReplicatedMapEntryViewHolder.class);
            entryViewHolders.add(entryViewHolder);
        }
        setPartitionId(in.readInt());
    }

    @Override
    public String getName() {
        return name;
    }
}
