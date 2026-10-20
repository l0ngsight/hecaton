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

package uk.org.hecaton.multimap.impl.operations;

import uk.org.hecaton.core.EntryEventType;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.map.impl.DataCollection;
import uk.org.hecaton.map.impl.MapEntries;
import uk.org.hecaton.multimap.impl.MultiMapContainer;
import uk.org.hecaton.multimap.impl.MultiMapDataSerializerHook;
import uk.org.hecaton.multimap.impl.MultiMapRecord;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.operationservice.BackupAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

public class PutAllOperation extends AbstractMultiMapOperation implements MutatingOperation, BackupAwareOperation {
    private MapEntries mapEntries;

    public PutAllOperation() {
        super();
    }

    public PutAllOperation(String name, MapEntries mapEntries) {
        super(name);
        this.mapEntries = mapEntries;
        //NB: general structure copied from c.hz.map.impl.operation.PutAllOperation
    }

    @Override
    public final void run() {
        int size = mapEntries != null ? mapEntries.size() : 0;
        int currentIndex = 0;
        while (currentIndex < size) {
            Data dataKey = mapEntries.getKey(currentIndex);
            Data value = mapEntries.getValue(currentIndex);
            put(dataKey, value);
            currentIndex++;
        }
    }

    protected void put(Data dataKey, Data dataValue) {
        MultiMapContainer container = getOrCreateContainer();
        Collection<Data> c = ((DataCollection) toObject(dataValue)).getCollection();
        Collection<MultiMapRecord> coll = container.getOrCreateMultiMapValue(dataKey).getCollection(false);
        Iterator<Data> it = c.iterator();

        while (it.hasNext()) {
            Data o = it.next();
            MultiMapRecord record = new MultiMapRecord(container.nextId(), isBinary() ? o : toObject(o));
            if (coll.add(record)) {
                //NB: cant put this in afterRun because we want to notify on each new value
                getOrCreateContainer().update();
                publishEvent(EntryEventType.ADDED, dataKey, o, null);
                response = true;
            }
            //its potentially feasible to publish an event in the else case
            //and publish an event that a supplied value was not added-discarded
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(mapEntries);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        mapEntries = in.readObject();
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.PUT_ALL;
    }


    @Override
    public Operation getBackupOperation() {
        return new PutAllBackupOperation(name, mapEntries);
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }
}
