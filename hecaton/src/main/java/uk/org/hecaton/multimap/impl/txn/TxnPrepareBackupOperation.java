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

import uk.org.hecaton.internal.util.UUIDSerializationUtil;
import uk.org.hecaton.multimap.impl.MultiMapContainer;
import uk.org.hecaton.multimap.impl.MultiMapDataSerializerHook;
import uk.org.hecaton.multimap.impl.operations.AbstractKeyBasedMultiMapOperation;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.operationservice.BackupOperation;
import uk.org.hecaton.transaction.TransactionException;

import java.io.IOException;
import java.util.UUID;

import static uk.org.hecaton.multimap.impl.txn.TxnPrepareOperation.LOCK_EXTENSION_TIME_IN_MILLIS;

public class TxnPrepareBackupOperation extends AbstractKeyBasedMultiMapOperation implements BackupOperation {

    private UUID caller;

    public TxnPrepareBackupOperation() {
    }

    public TxnPrepareBackupOperation(String name, Data dataKey, long threadId, UUID caller) {
        super(name, dataKey, threadId);
        this.caller = caller;
    }

    @Override
    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainerWithoutAccess();
        if (!container.txnLock(dataKey, caller, threadId, getCallId(), LOCK_EXTENSION_TIME_IN_MILLIS, true)) {
            throw new TransactionException(
                    "Lock is not owned by the transaction! -> " + container.getLockOwnerInfo(dataKey)
            );
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        UUIDSerializationUtil.writeUUID(out, caller);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        caller = UUIDSerializationUtil.readUUID(in);
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.TXN_PREPARE_BACKUP;
    }
}
