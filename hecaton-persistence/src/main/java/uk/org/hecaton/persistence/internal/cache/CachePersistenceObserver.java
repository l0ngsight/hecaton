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
package uk.org.hecaton.persistence.internal.cache;

import uk.org.hecaton.cache.impl.CacheRecordObserver;
import uk.org.hecaton.cache.impl.ICacheRecordStore;
import uk.org.hecaton.cache.impl.record.CacheRecord;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.persistence.internal.DataStore;
import uk.org.hecaton.persistence.internal.store.PartitionLog;
import uk.org.hecaton.persistence.internal.store.StructureRegistry.Structure;

/**
 * Writes the record changes of one partition's record store of a cache to the partition's log.
 * Records are kept in their serialized form. Runs on the partition's thread.
 */
public final class CachePersistenceObserver implements CacheRecordObserver {

    private final DataStore store;
    private final SerializationService serializationService;
    private final int partitionId;
    private final int structureId;
    private final boolean fsync;

    public CachePersistenceObserver(DataStore store, ICacheRecordStore recordStore,
                                    SerializationService serializationService) {
        this.store = store;
        this.serializationService = serializationService;
        this.partitionId = recordStore.getPartitionId();
        this.structureId = store.structureId(Structure.CACHE, recordStore.getName());
        this.fsync = recordStore.getConfig().getDataPersistenceConfig().isFsync();
        store.saveConfig(structureId, serializationService.toData(recordStore.getConfig()).toByteArray());
    }

    @Override
    public void onPut(Data key, CacheRecord record) {
        if (store.isLoading(partitionId)) {
            return;
        }
        PartitionLog log = store.log(partitionId);
        log.put(structureId, key.toByteArray(), serializationService.toData(record).toByteArray());
        forceIfNeeded(log);
    }

    @Override
    public void onRemove(Data key) {
        if (store.isLoading(partitionId)) {
            return;
        }
        PartitionLog log = store.log(partitionId);
        log.remove(structureId, key.toByteArray());
        forceIfNeeded(log);
    }

    @Override
    public void onClear() {
        if (store.isLoading(partitionId)) {
            return;
        }
        PartitionLog log = store.log(partitionId);
        log.clear(structureId);
        forceIfNeeded(log);
    }

    private void forceIfNeeded(PartitionLog log) {
        if (fsync) {
            log.force();
        }
    }
}
