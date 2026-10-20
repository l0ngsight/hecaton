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

import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.map.impl.MapEntries;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapService;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedRecord;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedRecordStore;
import uk.org.hecaton.spi.impl.operationservice.ReadonlyOperation;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class EntrySetOperation extends AbstractNamedSerializableOperation implements ReadonlyOperation {

    private String name;

    private transient Object response;

    public EntrySetOperation() {
    }

    public EntrySetOperation(String name) {
        this.name = name;
    }

    @Override
    public void run() throws Exception {
        ReplicatedMapService service = getService();
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(name);
        List<Map.Entry<Object, ReplicatedRecord>> entries = new ArrayList<>();
        for (ReplicatedRecordStore store : stores) {
            entries.addAll(store.entrySet(false));
        }
        ArrayList<Map.Entry<Data, Data>> dataEntries = new ArrayList<>(entries.size());
        SerializationService serializationService = getNodeEngine().getSerializationService();
        for (Map.Entry<Object, ReplicatedRecord> entry : entries) {
            Data key = serializationService.toData(entry.getKey());
            Data value = serializationService.toData(entry.getValue().getValue());
            dataEntries.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
        }
        response = new MapEntries(dataEntries);
    }

    @Override
    public Object getResponse() {
        return response;
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
        return ReplicatedMapDataSerializerHook.ENTRY_SET;
    }

    @Override
    public String getName() {
        return name;
    }
}
