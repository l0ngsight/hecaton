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
import uk.org.hecaton.core.ItemEventType;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.util.Map;

public class CollectionClearOperation extends CollectionBackupAwareOperation implements MutatingOperation {

    private Map<Long, Data> itemIdMap;

    public CollectionClearOperation() {
    }

    public CollectionClearOperation(String name) {
        super(name);
    }

    @Override
    public boolean shouldBackup() {
        return itemIdMap != null && !itemIdMap.isEmpty();
    }

    @Override
    public Operation getBackupOperation() {
        return new CollectionClearBackupOperation(name, itemIdMap.keySet());
    }

    @Override
    public void run() throws Exception {
        CollectionContainer container = getOrCreateContainer();
        itemIdMap = container.clear(true);
    }

    @Override
    public void afterRun() throws Exception {
        for (Data value : itemIdMap.values()) {
            publishEvent(ItemEventType.REMOVED, value);
        }
        super.afterRun();
    }

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.COLLECTION_CLEAR;
    }
}
