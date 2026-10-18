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
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.operationservice.BackupOperation;
import uk.org.hecaton.internal.serialization.impl.SerializationUtil;
import uk.org.hecaton.internal.services.RemoteService;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicy;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Creates backups for merged collection items after split-brain healing with a {@link SplitBrainMergePolicy}.
 *
 * @since 3.10
 */
public class CollectionMergeBackupOperation extends CollectionOperation implements BackupOperation {

    private Collection<CollectionItem> backupItems;

    public CollectionMergeBackupOperation() {
    }

    public CollectionMergeBackupOperation(String name, Collection<CollectionItem> backupItems) {
        super(name);
        this.backupItems = backupItems;
    }

    @Override
    public void run() throws Exception {
        CollectionContainer container = getOrCreateContainer();
        if (backupItems.isEmpty()) {
            RemoteService service = getService();
            service.destroyDistributedObject(name);
        } else {
            Map<Long, CollectionItem> backupMap = container.getMap();
            backupMap.clear();
            for (CollectionItem backupItem : backupItems) {
                backupMap.put(backupItem.getItemId(), backupItem);
            }
        }
    }

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.COLLECTION_MERGE_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        SerializationUtil.writeCollection(backupItems, out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        backupItems = SerializationUtil.readCollection(in);
    }
}
