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

package uk.org.hecaton.collection.impl.queue.operations;

import uk.org.hecaton.collection.impl.queue.QueueContainer;
import uk.org.hecaton.collection.impl.queue.QueueDataSerializerHook;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.operationservice.BackupOperation;

import java.io.IOException;
import java.util.Map;

import static uk.org.hecaton.internal.util.MapUtil.createHashMap;

/**
 * Provides backup functionality for {@link AddAllOperation}
 */
public class AddAllBackupOperation extends QueueOperation implements BackupOperation {

    private Map<Long, Data> dataMap;

    public AddAllBackupOperation() {
    }

    public AddAllBackupOperation(String name, Map<Long, Data> dataMap) {
        super(name);
        this.dataMap = dataMap;
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        queueContainer.addAllBackup(dataMap);
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.ADD_ALL_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(dataMap.size());
        for (Map.Entry<Long, Data> entry : dataMap.entrySet()) {
            long itemId = entry.getKey();
            Data value = entry.getValue();
            out.writeLong(itemId);
            IOUtil.writeData(out, value);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        dataMap = createHashMap(size);
        for (int i = 0; i < size; i++) {
            long itemId = in.readLong();
            Data value = IOUtil.readData(in);
            dataMap.put(itemId, value);
        }
    }
}
