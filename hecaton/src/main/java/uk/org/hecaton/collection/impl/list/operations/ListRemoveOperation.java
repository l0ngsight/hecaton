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

package uk.org.hecaton.collection.impl.list.operations;

import uk.org.hecaton.collection.impl.collection.CollectionDataSerializerHook;
import uk.org.hecaton.collection.impl.collection.CollectionItem;
import uk.org.hecaton.collection.impl.collection.operations.CollectionBackupAwareOperation;
import uk.org.hecaton.collection.impl.collection.operations.CollectionRemoveBackupOperation;
import uk.org.hecaton.collection.impl.list.ListContainer;
import uk.org.hecaton.core.ItemEventType;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;

public class ListRemoveOperation extends CollectionBackupAwareOperation implements MutatingOperation {

    private int index;
    private long itemId;

    public ListRemoveOperation() {
    }

    public ListRemoveOperation(String name, int index) {
        super(name);
        this.index = index;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        return new CollectionRemoveBackupOperation(name, itemId);
    }

    @Override
    public void beforeRun() throws Exception {
        publishEvent(ItemEventType.REMOVED, (Data) response);
    }

    @Override
    public void run() throws Exception {
        ListContainer listContainer = getOrCreateListContainer();
        final CollectionItem item = listContainer.remove(index);
        itemId = item.getItemId();
        response = item.getValue();
    }

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.LIST_REMOVE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(index);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        index = in.readInt();
    }
}
