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

package uk.org.hecaton.map.impl.operation;

import uk.org.hecaton.internal.iteration.IterationPointer;
import uk.org.hecaton.map.impl.MapDataSerializerHook;
import uk.org.hecaton.map.impl.iterator.MapKeysWithCursor;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.operationservice.ReadonlyOperation;

import java.io.IOException;

/**
 * Operation for fetching a chunk of keys from a single
 * {@link uk.org.hecaton.map.IMap} partition.
 * The iteration state is defined by the {@link #pointers} and the soft
 * limit is defined by the {@link #fetchSize}.
 *
 * @see uk.org.hecaton.map.impl.proxy.MapProxyImpl#iterator(int, int, boolean)
 */
public class MapFetchKeysOperation extends MapOperation implements ReadonlyOperation {

    private int fetchSize;
    private IterationPointer[] pointers;
    private transient MapKeysWithCursor response;

    public MapFetchKeysOperation() {
    }

    //@SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "This is an internal class")
    public MapFetchKeysOperation(String name, IterationPointer[] pointers, int fetchSize) {
        super(name);
        this.pointers = pointers;
        this.fetchSize = fetchSize;
    }

    @Override
    protected void runInternal() {
        response = recordStore.fetchKeys(pointers, fetchSize);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        fetchSize = in.readInt();
        int pointersCount = in.readInt();
        pointers = new IterationPointer[pointersCount];
        for (int i = 0; i < pointersCount; i++) {
            pointers[i] = new IterationPointer(in.readInt(), in.readInt());
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(fetchSize);
        out.writeInt(pointers.length);
        for (IterationPointer pointer : pointers) {
            out.writeInt(pointer.getIndex());
            out.writeInt(pointer.getSize());
        }
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.FETCH_KEYS;
    }
}
