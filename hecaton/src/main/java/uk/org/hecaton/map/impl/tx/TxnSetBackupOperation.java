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

package uk.org.hecaton.map.impl.tx;

import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.util.UUIDSerializationUtil;
import uk.org.hecaton.map.impl.MapDataSerializerHook;
import uk.org.hecaton.map.impl.operation.PutBackupOperation;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.record.Records;
import uk.org.hecaton.map.impl.recordstore.expiry.ExpiryMetadata;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.UUID;


public class TxnSetBackupOperation extends PutBackupOperation {
    private UUID transactionId;

    public TxnSetBackupOperation() {
    }

    public TxnSetBackupOperation(String name, Data dataKey, Record<Data> record,
                                 Data dataValue, ExpiryMetadata expiryMetadata, UUID transactionId) {
        super(name, dataKey, record, dataValue, expiryMetadata);
        this.transactionId = transactionId;
    }

    @Override
    protected void runInternal() {
        Record currentRecord = recordStore.putBackupTxn(dataKey, record,
                expiryMetadata, isPutTransient(), getCallerProvenance(), transactionId);
        Records.copyMetadataFrom(record, currentRecord);
        recordStore.forceUnlock(dataKey);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        UUIDSerializationUtil.writeUUID(out, transactionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        transactionId = UUIDSerializationUtil.readUUID(in);
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.TXN_SET_BACKUP;
    }
}
