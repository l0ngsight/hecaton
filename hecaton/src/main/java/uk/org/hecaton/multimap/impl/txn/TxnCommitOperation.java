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

import uk.org.hecaton.multimap.impl.MultiMapDataSerializerHook;
import uk.org.hecaton.multimap.impl.operations.AbstractBackupAwareMultiMapOperation;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.impl.SerializationUtil;
import uk.org.hecaton.spi.impl.operationservice.BackupAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.Notifier;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.WaitNotifyKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static uk.org.hecaton.spi.impl.operationexecutor.OperationRunner.runDirect;

public class TxnCommitOperation extends AbstractBackupAwareMultiMapOperation implements Notifier {

    private List<Operation> opList;

    private transient boolean notify = true;

    public TxnCommitOperation() {
    }

    public TxnCommitOperation(int partitionId, String name, Data dataKey, long threadId, List<Operation> opList) {
        super(name, dataKey, threadId);
        setPartitionId(partitionId);
        this.opList = opList;
    }

    @Override
    public void run() throws Exception {
        for (Operation op : opList) {
            op.setNodeEngine(getNodeEngine())
                    .setServiceName(getServiceName())
                    .setPartitionId(getPartitionId());
            runDirect(op);
        }
        getOrCreateContainer().unlock(dataKey, getCallerUuid(), threadId, getCallId());
    }

    @Override
    public boolean shouldBackup() {
        return notify;
    }

    @Override
    public Operation getBackupOperation() {
        List<Operation> backupOpList = new ArrayList<>();
        for (Operation operation : opList) {
            if (operation instanceof BackupAwareOperation backupAwareOperation) {
                if (backupAwareOperation.shouldBackup()) {
                    backupOpList.add(backupAwareOperation.getBackupOperation());
                }
            }
        }
        return new TxnCommitBackupOperation(name, dataKey, backupOpList, getCallerUuid(), threadId);
    }

    @Override
    public boolean shouldNotify() {
        return notify;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return getWaitKey();
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.TXN_COMMIT;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        SerializationUtil.writeList(opList, out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        opList = SerializationUtil.readList(in);
    }
}
