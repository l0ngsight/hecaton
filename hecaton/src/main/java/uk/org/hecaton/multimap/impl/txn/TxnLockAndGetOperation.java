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

package uk.org.hecaton.multimap.impl.txn;

import uk.org.hecaton.internal.locksupport.LockWaitNotifyKey;
import uk.org.hecaton.multimap.impl.MultiMapContainer;
import uk.org.hecaton.multimap.impl.MultiMapDataSerializerHook;
import uk.org.hecaton.multimap.impl.MultiMapRecord;
import uk.org.hecaton.multimap.impl.MultiMapService;
import uk.org.hecaton.multimap.impl.MultiMapValue;
import uk.org.hecaton.multimap.impl.operations.AbstractKeyBasedMultiMapOperation;
import uk.org.hecaton.multimap.impl.operations.MultiMapResponse;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.operationservice.BlockingOperation;
import uk.org.hecaton.internal.services.DistributedObjectNamespace;
import uk.org.hecaton.spi.impl.operationservice.WaitNotifyKey;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;
import uk.org.hecaton.transaction.TransactionException;

import java.io.IOException;
import java.util.Collection;

public class TxnLockAndGetOperation extends AbstractKeyBasedMultiMapOperation implements BlockingOperation, MutatingOperation {

    private long ttl;
    private boolean blockReads;

    public TxnLockAndGetOperation() {
    }

    public TxnLockAndGetOperation(String name, Data dataKey, long timeout, long ttl, long threadId, boolean blockReads) {
        super(name, dataKey);
        this.ttl = ttl;
        this.threadId = threadId;
        this.blockReads = blockReads;
        setWaitTimeout(timeout);
    }

    @Override
    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        if (!container.txnLock(dataKey, getCallerUuid(), threadId, getCallId(), ttl, blockReads)) {
            throw new TransactionException("Transaction couldn't obtain lock!");
        }
        MultiMapValue multiMapValue = container.getMultiMapValueOrNull(dataKey);
        boolean isLocal = executedLocally();
        Collection<MultiMapRecord> collection = multiMapValue == null ? null : multiMapValue.getCollection(isLocal);
        MultiMapResponse multiMapResponse = new MultiMapResponse(collection, getValueCollectionType(container));
        multiMapResponse.setNextRecordId(container.nextId());
        response = multiMapResponse;
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new DistributedObjectNamespace(MultiMapService.SERVICE_NAME, name), dataKey);
    }

    @Override
    public boolean shouldWait() {
        return !getOrCreateContainer().canAcquireLock(dataKey, getCallerUuid(), threadId);
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(ttl);
        out.writeBoolean(blockReads);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        ttl = in.readLong();
        blockReads = in.readBoolean();
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.TXN_LOCK_AND_GET;
    }
}
