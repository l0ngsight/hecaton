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

package uk.org.hecaton.collection.impl.txnqueue.operations;

import uk.org.hecaton.collection.impl.queue.QueueContainer;
import uk.org.hecaton.collection.impl.queue.QueueDataSerializerHook;
import uk.org.hecaton.collection.impl.queue.operations.QueueBackupAwareOperation;
import uk.org.hecaton.internal.util.UUIDSerializationUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.UUID;

/**
 * Prepare operation for the transactional queue.
 */
public class TxnPrepareOperation extends QueueBackupAwareOperation {

    private long[] itemIds;
    private UUID transactionId;

    public TxnPrepareOperation() {
    }

    public TxnPrepareOperation(int partitionId, String name, long[] itemIds, UUID transactionId) {
        super(name);
        setPartitionId(partitionId);
        this.itemIds = itemIds;
        this.transactionId = transactionId;
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        for (long itemId : itemIds) {
            queueContainer.txnCheckReserve(Math.abs(itemId));
        }
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        return new TxnPrepareBackupOperation(name, itemIds, transactionId);
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.TXN_PREPARE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        UUIDSerializationUtil.writeUUID(out, transactionId);
        out.writeLongArray(itemIds);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        transactionId = UUIDSerializationUtil.readUUID(in);
        itemIds = in.readLongArray();
    }
}
