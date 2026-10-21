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
package uk.org.hecaton.persistence.internal.map;

import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.recordstore.MutationObserver;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.persistence.internal.DataStore;
import uk.org.hecaton.persistence.internal.store.PartitionLog;
import uk.org.hecaton.persistence.internal.store.StructureRegistry.Structure;

import javax.annotation.Nonnull;

/**
 * Writes the changes of one partition's record store of a map to the partition's log. Runs on the
 * partition's thread.
 */
public final class MapPersistenceObserver implements MutationObserver<Record> {

    private final DataStore store;
    private final RecordStore<Record> recordStore;
    private final SerializationService serializationService;
    private final int partitionId;
    private final int structureId;
    private final boolean fsync;

    public MapPersistenceObserver(DataStore store, RecordStore<Record> recordStore,
                                  SerializationService serializationService) {
        this.store = store;
        this.recordStore = recordStore;
        this.serializationService = serializationService;
        this.partitionId = recordStore.getPartitionId();
        this.structureId = store.structureId(Structure.MAP, recordStore.getName());
        this.fsync = recordStore.getMapContainer().getMapConfig().getDataPersistenceConfig().isFsync();
    }

    @Override
    public void onPutRecord(@Nonnull Data key, Record record, Object oldValue, boolean backup) {
        write(key, record);
    }

    @Override
    public void onReplicationPutRecord(@Nonnull Data key, @Nonnull Record record, boolean populateIndex) {
        write(key, record);
    }

    @Override
    public void onUpdateRecord(@Nonnull Data key, @Nonnull Record record, Object oldValue, Object newValue,
                               boolean backup) {
        write(key, record);
    }

    @Override
    public void onRemoveRecord(@Nonnull Data key, Record record, boolean backup) {
        remove(key);
    }

    @Override
    public void onEvictRecord(@Nonnull Data key, @Nonnull Record record, boolean backup) {
        remove(key);
    }

    @Override
    public void onLoadRecord(@Nonnull Data key, @Nonnull Record record, boolean backup) {
        write(key, record);
    }

    @Override
    public void onReset() {
        clear();
    }

    @Override
    public void onClear() {
        clear();
    }

    @Override
    public void onDestroy(boolean isDuringShutdown, boolean internal) {
        // the data stays on disk when the member shuts down
        if (!isDuringShutdown) {
            clear();
        }
    }

    private void write(Data key, Record record) {
        if (store.isLoading(partitionId)) {
            return;
        }
        PartitionLog log = store.log(partitionId);
        log.put(structureId, key.toByteArray(),
                MapRecordCodec.encode(record, recordStore.getExpirySystem().getExpiryMetadata(key), serializationService));
        forceIfNeeded(log);
    }

    private void remove(Data key) {
        if (store.isLoading(partitionId)) {
            return;
        }
        PartitionLog log = store.log(partitionId);
        log.remove(structureId, key.toByteArray());
        forceIfNeeded(log);
    }

    private void clear() {
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
