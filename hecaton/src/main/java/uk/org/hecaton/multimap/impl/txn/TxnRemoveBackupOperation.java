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

import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.multimap.impl.MultiMapContainer;
import uk.org.hecaton.multimap.impl.MultiMapDataSerializerHook;
import uk.org.hecaton.multimap.impl.MultiMapRecord;
import uk.org.hecaton.multimap.impl.MultiMapValue;
import uk.org.hecaton.multimap.impl.operations.AbstractKeyBasedMultiMapOperation;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.operationservice.BackupOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

public class TxnRemoveBackupOperation extends AbstractKeyBasedMultiMapOperation implements BackupOperation {

    private long recordId;
    private Data value;

    public TxnRemoveBackupOperation() {
    }

    public TxnRemoveBackupOperation(String name, Data dataKey, long recordId, Data value) {
        super(name, dataKey);
        this.recordId = recordId;
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainerWithoutAccess();
        MultiMapValue multiMapValue = container.getMultiMapValueOrNull(dataKey);
        if (multiMapValue == null || !multiMapValue.containsRecordId(recordId)) {
            response = false;
            return;
        }
        response = true;
        Collection<MultiMapRecord> coll = multiMapValue.getCollection(false);
        Iterator<MultiMapRecord> iterator = coll.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().getRecordId() == recordId) {
                iterator.remove();
                break;
            }
        }
        if (coll.isEmpty()) {
            container.delete(dataKey);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(recordId);
        IOUtil.writeData(out, value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        recordId = in.readLong();
        value = IOUtil.readData(in);
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.TXN_REMOVE_BACKUP;
    }
}
