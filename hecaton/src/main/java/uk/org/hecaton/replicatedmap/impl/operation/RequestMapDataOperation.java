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

import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.replicatedmap.impl.PartitionContainer;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapService;
import uk.org.hecaton.replicatedmap.impl.record.RecordMigrationInfo;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedRecord;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedRecordStore;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.internal.serialization.SerializationService;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import static uk.org.hecaton.replicatedmap.impl.ReplicatedMapService.INVOCATION_TRY_COUNT;
import static uk.org.hecaton.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;
import static uk.org.hecaton.internal.util.SetUtil.createHashSet;

/**
 * Collects and sends the replicated map data from the executing node to the caller via
 * {@link SyncReplicatedMapDataOperation}.
 */
public class RequestMapDataOperation extends AbstractSerializableOperation {

    private String name;

    public RequestMapDataOperation() {
    }

    public RequestMapDataOperation(String name) {
        this.name = name;
    }

    @Override
    public void run() throws Exception {
        ILogger logger = getLogger();
        Address callerAddress = getCallerAddress();
        int partitionId = getPartitionId();
        NodeEngine nodeEngine = getNodeEngine();
        if (logger.isFineEnabled()) {
            logger.fine("Caller " + callerAddress + " requested copy of replicated map '" + name
                    + "' (partitionId " + partitionId + ") from " + nodeEngine.getThisAddress());
        }
        ReplicatedMapService service = getService();
        PartitionContainer container = service.getPartitionContainer(partitionId);
        ReplicatedRecordStore store = container.getOrCreateRecordStore(name);
        store.setLoaded(true);

        if (nodeEngine.getThisAddress().equals(callerAddress)) {
            return;
        }

        long version = store.getVersion();
        Set<RecordMigrationInfo> recordSet = getRecordSet(store);
        Operation op = new SyncReplicatedMapDataOperation(name, recordSet, version)
                .setPartitionId(partitionId)
                .setValidateTarget(false);
        OperationService operationService = nodeEngine.getOperationService();
        operationService.createInvocationBuilder(SERVICE_NAME, op, callerAddress)
                .setTryCount(INVOCATION_TRY_COUNT)
                .invoke();
    }

    private Set<RecordMigrationInfo> getRecordSet(ReplicatedRecordStore store) {
        SerializationService serializationService = getNodeEngine().getSerializationService();
        Set<RecordMigrationInfo> recordSet = createHashSet(store.size());
        Iterator<ReplicatedRecord> iterator = store.recordIterator();
        while (iterator.hasNext()) {
            ReplicatedRecord record = iterator.next();
            Data dataKey = serializationService.toData(record.getKeyInternal());
            Data dataValue = serializationService.toData(record.getValueInternal());
            recordSet.add(new RecordMigrationInfo(dataKey, dataValue, record.getTtlMillis()));
        }
        return recordSet;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readString();
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.REQUEST_MAP_DATA;
    }
}
