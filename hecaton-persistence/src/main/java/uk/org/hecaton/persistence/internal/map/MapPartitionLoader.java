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

import uk.org.hecaton.config.CacheDeserializedValues;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.impl.HeapData;
import uk.org.hecaton.internal.util.Clock;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.PartitionContainer;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.record.Records;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.query.impl.CachedQueryEntry;
import uk.org.hecaton.query.impl.Index;
import uk.org.hecaton.query.impl.IndexRegistry;
import uk.org.hecaton.query.impl.InternalIndex;
import uk.org.hecaton.query.impl.QueryableEntry;
import uk.org.hecaton.spi.impl.NodeEngine;

import java.util.LinkedHashSet;
import java.util.Set;

import static uk.org.hecaton.config.CacheDeserializedValues.NEVER;

/**
 * Loads the persisted map entries of one partition into the record stores, and builds the indexes
 * of the partitions the member owns.
 */
public final class MapPartitionLoader {

    private final MapServiceContext mapServiceContext;
    private final SerializationService serializationService;
    private final int partitionId;
    private final Set<RecordStore<Record>> loaded = new LinkedHashSet<>();
    private final long now = Clock.currentTimeMillis();

    public MapPartitionLoader(NodeEngine nodeEngine, int partitionId) {
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        this.mapServiceContext = mapService.getMapServiceContext();
        this.serializationService = nodeEngine.getSerializationService();
        this.partitionId = partitionId;
    }

    /** Puts a persisted entry into its record store. */
    @SuppressWarnings("unchecked")
    public void load(String mapName, byte[] key, byte[] value) {
        PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);
        RecordStore<Record> recordStore = container.getRecordStoreForHotRestart(mapName);
        MapRecordCodec.Decoded decoded = MapRecordCodec.decode(value);
        Data dataKey = new HeapData(key);
        recordStore.beforeOperation();
        try {
            Record record = recordStore.createRecord(dataKey, decoded.value(), now);
            record.setVersion(decoded.version());
            record.setCreationTime(decoded.creationTime());
            record.setLastUpdateTime(decoded.lastUpdateTime());
            recordStore.putOrUpdateReplicatedRecord(dataKey, record, decoded.expiry(), false, now);
            recordStore.disposeDeferredBlocks();
        } finally {
            recordStore.afterOperation();
        }
        loaded.add(recordStore);
    }

    /** Builds the indexes of the loaded record stores; for partitions the member owns. */
    public void populateIndexes() {
        for (RecordStore<Record> recordStore : loaded) {
            MapContainer mapContainer = recordStore.getMapContainer();
            IndexRegistry indexRegistry = mapContainer.getOrCreateIndexRegistry(partitionId);
            recordStore.beforeOperation();
            try {
                indexRegistry.createIndexesFromRecordedDefinitions();
            } finally {
                recordStore.afterOperation();
            }
            if (!indexRegistry.haveAtLeastOneIndex()) {
                continue;
            }
            InternalIndex[] indexes = indexRegistry.getIndexes();
            IndexRegistry.beginPartitionUpdate(indexes);
            CacheDeserializedValues cacheDeserializedValues = mapContainer.getMapConfig().getCacheDeserializedValues();
            CachedQueryEntry<?, ?> cachedEntry = cacheDeserializedValues == NEVER
                    ? new CachedQueryEntry<>(serializationService, mapContainer.getExtractors()) : null;
            recordStore.beforeOperation();
            try {
                recordStore.forEach((key, record) -> {
                    Object value = Records.getValueOrCachedValue(record, serializationService);
                    if (value != null) {
                        QueryableEntry queryEntry = mapContainer.newQueryEntry(key, value);
                        queryEntry.setRecord(record);
                        CachedQueryEntry<?, ?> newEntry =
                                cachedEntry == null ? (CachedQueryEntry<?, ?>) queryEntry : cachedEntry.init(key, value);
                        indexRegistry.putEntry(newEntry, null, queryEntry, Index.OperationSource.SYSTEM);
                    }
                }, false);
            } finally {
                recordStore.afterOperation();
            }
            IndexRegistry.markPartitionAsIndexed(partitionId, indexes);
        }
    }
}
