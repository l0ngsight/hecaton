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
import uk.org.hecaton.map.impl.MapEntries;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapEventPublishingService;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapService;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedRecordStore;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationService;

import java.io.IOException;
import java.util.Collection;

import static uk.org.hecaton.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;

/**
 * Puts a set of records to the replicated map.
 */
public class PutAllOperation extends AbstractNamedSerializableOperation implements MutatingOperation {

    private String name;
    private MapEntries entries;

    public PutAllOperation() {
    }

    public PutAllOperation(String name, MapEntries entries) {
        this.name = name;
        this.entries = entries;
    }

    @Override
    public void run() throws Exception {
        ReplicatedMapService service = getService();
        ReplicatedRecordStore store = service.getReplicatedRecordStore(name, true, getPartitionId());
        int partitionId = getPartitionId();
        IPartitionService partitionService = getNodeEngine().getPartitionService();
        ReplicatedMapEventPublishingService eventPublishingService = service.getEventPublishingService();
        for (int i = 0; i < entries.size(); i++) {
            Data key = entries.getKey(i);
            Data value = entries.getValue(i);
            if (partitionId != partitionService.getPartitionId(key)) {
                continue;
            }
            Object putResult = store.put(key, value);
            Data oldValue = getNodeEngine().toData(putResult);
            eventPublishingService.fireEntryListenerEvent(key, oldValue, value, name, getCallerAddress());
            VersionResponsePair response = new VersionResponsePair(putResult, store.getVersion());
            publishReplicationMessage(key, value, response);
        }
    }

    private void publishReplicationMessage(Data key, Data value, VersionResponsePair response) {
        OperationService operationService = getNodeEngine().getOperationService();
        Collection<Member> members = getNodeEngine().getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        for (Member member : members) {
            Address address = member.getAddress();
            if (address.equals(getNodeEngine().getThisAddress())) {
                continue;
            }
            Operation op = new ReplicateUpdateOperation(name, key, value, 0, response, false, getCallerAddress())
                    .setPartitionId(getPartitionId())
                    .setValidateTarget(false);
            operationService.invokeOnTarget(getServiceName(), op, address);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeObject(entries);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readString();
        entries = in.readObject();
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.PUT_ALL;
    }

    @Override
    public String getName() {
        return name;
    }
}
