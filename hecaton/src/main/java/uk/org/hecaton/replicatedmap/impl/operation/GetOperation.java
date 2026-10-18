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

import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapService;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedRecord;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedRecordStore;
import uk.org.hecaton.spi.impl.operationservice.ReadonlyOperation;

import java.io.IOException;

public class GetOperation extends AbstractNamedSerializableOperation implements ReadonlyOperation {

    private String name;
    private Data key;
    private transient Object response;

    public GetOperation() {
    }

    public GetOperation(String name, Data key) {
        this.name = name;
        this.key = key;
    }

    @Override
    public void run() throws Exception {
        ReplicatedMapService service = getService();
        ReplicatedRecordStore store = service.getReplicatedRecordStore(name, false, getPartitionId());
        if (store != null) {
            ReplicatedRecord record = store.getReplicatedRecord(key);
            if (record != null) {
                response = record.getValue();
            }
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        IOUtil.writeData(out, key);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readString();
        key = IOUtil.readData(in);
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.GET;
    }

    @Override
    public String getName() {
        return name;
    }
}
