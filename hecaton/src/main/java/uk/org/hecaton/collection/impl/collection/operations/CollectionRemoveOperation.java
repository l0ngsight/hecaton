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

package uk.org.hecaton.collection.impl.collection.operations;

import uk.org.hecaton.collection.impl.collection.CollectionContainer;
import uk.org.hecaton.collection.impl.collection.CollectionDataSerializerHook;
import uk.org.hecaton.collection.impl.collection.CollectionItem;
import uk.org.hecaton.core.ItemEventType;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.io.IOException;

import static uk.org.hecaton.collection.impl.collection.CollectionContainer.INVALID_ITEM_ID;

public class CollectionRemoveOperation extends CollectionBackupAwareOperation implements MutatingOperation {

    private Data value;
    private long itemId = INVALID_ITEM_ID;

    public CollectionRemoveOperation() {
    }

    public CollectionRemoveOperation(String name, Data value) {
        super(name);
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        response = false;
        CollectionContainer collectionContainer = getOrCreateContainer();
        CollectionItem item = collectionContainer.remove(value);
        if (item != null) {
            response = true;
            itemId = item.getItemId();
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (itemId != INVALID_ITEM_ID) {
            publishEvent(ItemEventType.REMOVED, value);
        }
        super.afterRun();
    }

    @Override
    public boolean shouldBackup() {
        return itemId != INVALID_ITEM_ID;
    }

    @Override
    public Operation getBackupOperation() {
        return new CollectionRemoveBackupOperation(name, itemId);
    }

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.COLLECTION_REMOVE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeData(out, value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = IOUtil.readData(in);
    }
}
