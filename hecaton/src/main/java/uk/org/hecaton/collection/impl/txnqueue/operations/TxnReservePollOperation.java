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
import uk.org.hecaton.collection.impl.queue.QueueItem;
import uk.org.hecaton.collection.impl.queue.operations.QueueBackupAwareOperation;
import uk.org.hecaton.internal.util.UUIDSerializationUtil;
import uk.org.hecaton.transaction.TransactionalQueue;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.operationservice.BlockingOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.WaitNotifyKey;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;
import java.util.UUID;

/**
 * Transaction prepare operation for a queue poll, executed on the primary replica.
 * <p>
 * The operation can also wait until there is at least one item reserved or the
 * wait timeout has elapsed.
 *
 * @see TransactionalQueue#poll
 * @see TxnPollOperation
 */
public class TxnReservePollOperation extends QueueBackupAwareOperation implements BlockingOperation, MutatingOperation {

    private long reservedOfferId;
    private UUID transactionId;

    public TxnReservePollOperation() {
    }

    public TxnReservePollOperation(String name, long timeoutMillis, long reservedOfferId, UUID transactionId) {
        super(name, timeoutMillis);
        this.reservedOfferId = reservedOfferId;
        this.transactionId = transactionId;
    }

    @Override
    public void run() throws Exception {
        QueueContainer createContainer = getContainer();
        response = createContainer.txnPollReserve(reservedOfferId, transactionId);
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        QueueContainer queueContainer = getContainer();
        return queueContainer.getPollWaitNotifyKey();
    }

    @Override
    public boolean shouldWait() {
        final QueueContainer queueContainer = getContainer();
        return getWaitTimeout() != 0 && reservedOfferId == -1 && queueContainer.size() == 0;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }

    @Override
    public boolean shouldBackup() {
        return response != null;
    }

    @Override
    public Operation getBackupOperation() {
        final QueueItem item = (QueueItem) response;
        long itemId = item.getItemId();
        return new TxnReservePollBackupOperation(name, itemId, transactionId);
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.TXN_RESERVE_POLL;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(reservedOfferId);
        UUIDSerializationUtil.writeUUID(out, transactionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        reservedOfferId = in.readLong();
        transactionId = UUIDSerializationUtil.readUUID(in);
    }

}
