/*
 * Copyright (c) 2026, The Hecaton Authors.
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
package uk.org.hecaton.cp.internal.datastructures.cpmap;

import uk.org.hecaton.cp.internal.datastructures.spi.atomic.RaftAtomicValueSnapshot;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Snapshot of the {@link CPMapService} state of a Raft group.
 */
public class CPMapSnapshot extends RaftAtomicValueSnapshot<Map<Data, CPMapEntry>> {

    public CPMapSnapshot() {
    }

    public CPMapSnapshot(Map<String, Map<Data, CPMapEntry>> maps, Set<String> destroyed) {
        super(maps, destroyed);
    }

    @Override
    public int getFactoryId() {
        return CPMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CPMapDataSerializerHook.SNAPSHOT;
    }

    @Override
    protected void writeValue(ObjectDataOutput out, Map<Data, CPMapEntry> entries) throws IOException {
        out.writeInt(entries.size());
        for (Map.Entry<Data, CPMapEntry> e : entries.entrySet()) {
            IOUtil.writeData(out, e.getKey());
            IOUtil.writeData(out, e.getValue().value());
            out.writeLong(e.getValue().timestamp());
        }
    }

    @Override
    protected Map<Data, CPMapEntry> readValue(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        Map<Data, CPMapEntry> entries = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            Data key = IOUtil.readData(in);
            Data value = IOUtil.readData(in);
            entries.put(key, new CPMapEntry(value, in.readLong()));
        }
        return entries;
    }

    @Override
    public String toString() {
        return "CPMapSnapshot{maps=" + values.keySet() + ", destroyed=" + destroyed + '}';
    }
}
