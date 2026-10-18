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

package uk.org.hecaton.map.impl;

import uk.org.hecaton.config.CacheDeserializedValues;
import uk.org.hecaton.internal.nearcache.impl.invalidation.MetaDataGenerator;
import uk.org.hecaton.internal.partition.ChunkSupplier;
import uk.org.hecaton.internal.partition.ChunkSuppliers;
import uk.org.hecaton.internal.partition.ChunkedMigrationAwareService;
import uk.org.hecaton.internal.partition.MigrationEndpoint;
import uk.org.hecaton.internal.partition.OffloadedReplicationPreparation;
import uk.org.hecaton.internal.partition.PartitionMigrationEvent;
import uk.org.hecaton.internal.partition.PartitionReplicationEvent;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.internal.util.Clock;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.map.impl.operation.MapReplicationOperation;
import uk.org.hecaton.map.impl.querycache.QueryCacheContext;
import uk.org.hecaton.map.impl.querycache.publisher.PublisherContext;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.record.Records;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.query.impl.CachedQueryEntry;
import uk.org.hecaton.query.impl.Index;
import uk.org.hecaton.query.impl.IndexRegistry;
import uk.org.hecaton.query.impl.InternalIndex;
import uk.org.hecaton.query.impl.QueryableEntry;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import static uk.org.hecaton.config.CacheDeserializedValues.NEVER;
import static uk.org.hecaton.internal.partition.MigrationEndpoint.DESTINATION;
import static uk.org.hecaton.internal.partition.MigrationEndpoint.SOURCE;
import static uk.org.hecaton.map.impl.querycache.publisher.AccumulatorSweeper.flushAccumulator;
import static uk.org.hecaton.map.impl.querycache.publisher.AccumulatorSweeper.removeAccumulator;
import static uk.org.hecaton.map.impl.querycache.publisher.AccumulatorSweeper.sendEndOfSequenceEvents;

/**
 * Defines migration behavior of map service.
 *
 * @see MapService
 */
class MapMigrationAwareService
        implements ChunkedMigrationAwareService, OffloadedReplicationPreparation {

    protected final PartitionContainer[] containers;
    protected final MapServiceContext mapServiceContext;
    protected final SerializationService serializationService;

    private final ILogger logger;

    MapMigrationAwareService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.serializationService = mapServiceContext.getNodeEngine().getSerializationService();
        this.containers = mapServiceContext.getPartitionContainers();
        this.logger = mapServiceContext.getNodeEngine().getLogger(getClass());
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        return containers[event.getPartitionId()].getAllNamespaces(event.getReplicaIndex());
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        return namespace instanceof ObjectNamespace
                && MapService.SERVICE_NAME.equals(namespace.getServiceName());
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        if (isLocalPromotion(event)) {
            // It's a local partition promotion. We need to populate non-global indexes here since
            // there is no map replication performed in this case. Global indexes are populated
            // during promotion finalization phase.

            // 1. Defensively clear possible stale leftovers from the previous failed promotion attempt.
            clearPartitionedIndexes(event);

            // 2. Populate non-global partitioned indexes.
            populateIndexes(event, TargetIndexes.PARTITIONED, "beforeMigration");
        }

        flushAndRemoveQueryCaches(event);
    }

    /**
     * Flush and remove query cache on this source partition.
     */
    private void flushAndRemoveQueryCaches(PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        PublisherContext publisherContext = queryCacheContext.getPublisherContext();

        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            flushAccumulator(publisherContext, partitionId);
            removeAccumulator(publisherContext, partitionId);
            return;
        }

        if (isLocalPromotion(event)) {
            removeAccumulator(publisherContext, partitionId);
            sendEndOfSequenceEvents(publisherContext, partitionId);
            return;
        }
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return prepareReplicationOperation(event,
                containers[event.getPartitionId()].getAllNamespaces(event.getReplicaIndex()));
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                 Collection<ServiceNamespace> namespaces) {
        assert assertAllKnownNamespaces(namespaces);

        int partitionId = event.getPartitionId();

        Operation operation = new MapReplicationOperation(containers[partitionId],
                namespaces, partitionId, event.getReplicaIndex());
        operation.setService(mapServiceContext.getService());
        operation.setNodeEngine(mapServiceContext.getNodeEngine());

        return operation;
    }

    @Override
    public ChunkSupplier newChunkSupplier(PartitionReplicationEvent event,
                                          Collection<ServiceNamespace> namespaces) {
        List<ChunkSupplier> chain = new ArrayList<>(namespaces.size());
        for (ServiceNamespace namespace : namespaces) {
            chain.add(new MapChunkSupplier(mapServiceContext, namespace,
                    event.getPartitionId(), event.getReplicaIndex()));
        }

        return ChunkSuppliers.newChainedChunkSupplier(chain);
    }

    boolean assertAllKnownNamespaces(Collection<ServiceNamespace> namespaces) {
        for (ServiceNamespace namespace : namespaces) {
            assert isKnownServiceNamespace(namespace)
                    : namespace + " is not a MapService namespace!";
        }
        return true;
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == DESTINATION) {
            populateIndexes(event, TargetIndexes.GLOBAL, "commitMigration");
        } else {
            depopulateIndexes(event, "commitMigration");
        }

        PartitionContainer partitionContainer
                = mapServiceContext.getPartitionContainer(event.getPartitionId());
        if (SOURCE == event.getMigrationEndpoint()) {
            // Do not change order of below methods
            removeWbqCountersHavingLesserBackupCountThan(event.getPartitionId(),
                    event.getNewReplicaIndex());
            removeRecordStoresHavingLesserBackupCountThan(event.getPartitionId(),
                    event.getNewReplicaIndex());

            partitionContainer.cleanUpOnMigration(event.getNewReplicaIndex());
        }

        for (RecordStore recordStore : partitionContainer.getAllRecordStores()) {
            // in case the record store has been created without
            // loading during migration trigger again if loading
            // has been already started this call will do nothing
            recordStore.startLoading();
        }

        if (event.getCurrentReplicaIndex() == 0 || event.getNewReplicaIndex() == 0) {
            mapServiceContext.refreshCachedOwnedPartitions();
        }

        removeOrRegenerateNearCacheUuid(event);

    }

    private void removeOrRegenerateNearCacheUuid(PartitionMigrationEvent event) {
        if (SOURCE == event.getMigrationEndpoint()) {
            getMetaDataGenerator().removeUuidAndSequence(event.getPartitionId());
            return;
        }

        if (DESTINATION == event.getMigrationEndpoint() && event.getNewReplicaIndex() != 0) {
            getMetaDataGenerator().regenerateUuid(event.getPartitionId());
            return;
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (DESTINATION == event.getMigrationEndpoint()) {
            // Do not change order of below methods
            removeWbqCountersHavingLesserBackupCountThan(event.getPartitionId(),
                    event.getCurrentReplicaIndex());
            removeRecordStoresHavingLesserBackupCountThan(event.getPartitionId(),
                    event.getCurrentReplicaIndex());
            getMetaDataGenerator().removeUuidAndSequence(event.getPartitionId());

            PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(event.getPartitionId());
            partitionContainer.cleanUpOnMigration(event.getCurrentReplicaIndex());
        }

        if (event.getCurrentReplicaIndex() == 0 || event.getNewReplicaIndex() == 0) {
            mapServiceContext.refreshCachedOwnedPartitions();
        }
    }

    private void clearPartitionedIndexes(PartitionMigrationEvent event) {
        PartitionContainer container = mapServiceContext.getPartitionContainer(event.getPartitionId());

        for (RecordStore recordStore : container.getMaps().values()) {
            MapContainer mapContainer = recordStore.getMapContainer();
            IndexRegistry indexRegistry = mapContainer.getOrNullPartitionedIndexRegistry(event.getPartitionId());
            if (indexRegistry == null) {
                continue;
            }

            recordStore.beforeOperation();
            try {
                indexRegistry.clearAll();
            } finally {
                recordStore.afterOperation();
            }
        }
    }

    private void removeRecordStoresHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        if (thresholdReplicaIndex < 0) {
            mapServiceContext.removeRecordStoresFromPartitionMatchingWith(recordStore -> true, partitionId,
                    false, true);
        } else {
            mapServiceContext.removeRecordStoresFromPartitionMatchingWith(lesserBackupMapsThen(thresholdReplicaIndex),
                    partitionId, false, true);
        }
    }

    /**
     * Removes write-behind-queue-reservation-counters inside
     * supplied partition from matching record-stores.
     */
    private void removeWbqCountersHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        if (thresholdReplicaIndex < 0) {
            mapServiceContext.removeWbqCountersFromMatchingPartitionsWith(
                    recordStore -> true, partitionId);
        } else {
            mapServiceContext.removeWbqCountersFromMatchingPartitionsWith(
                    lesserBackupMapsThen(thresholdReplicaIndex), partitionId);
        }
    }

    /**
     * @param backupCount number of backups of a maps' partition
     * @return predicate to find all map partitions which are expected to have
     * fewer backups than given backupCount.
     */
    static Predicate<RecordStore> lesserBackupMapsThen(final int backupCount) {
        return recordStore -> recordStore.getMapContainer().getTotalBackupCount() < backupCount;
    }

    /**
     * @param backupCount number of backups of a maps' partition
     * @return predicate to find all map partitions which are expected to have
     * fewer backups than given backupCount.
     */
    static Predicate<MapContainer> lesserBackupMapsThenWithContainer(final int backupCount) {
        return container -> container.getTotalBackupCount() < backupCount;
    }

    private MetaDataGenerator getMetaDataGenerator() {
        return mapServiceContext.getMapNearCacheManager().getInvalidator().getMetaDataGenerator();
    }

    @SuppressWarnings("checkstyle:NPathComplexity")
    private void populateIndexes(PartitionMigrationEvent event,
                                 TargetIndexes targetIndexes, String stepName) {
        assert event.getMigrationEndpoint() == DESTINATION;
        assert targetIndexes != null;

        if (event.getNewReplicaIndex() != 0) {
            // backup partitions have no indexes to populate
            return;
        }

        PartitionContainer container = mapServiceContext.getPartitionContainer(event.getPartitionId());
        for (RecordStore<Record> recordStore : container.getMaps().values()) {
            MapContainer mapContainer = recordStore.getMapContainer();

            IndexRegistry indexRegistry = mapContainer.getOrCreateIndexRegistry(event.getPartitionId());
            recordStore.beforeOperation();
            try {
                indexRegistry.createIndexesFromRecordedDefinitions();
            } finally {
                recordStore.afterOperation();
            }
            if (!indexRegistry.haveAtLeastOneIndex()) {
                // no indexRegistry to work with
                continue;
            }

            if (indexRegistry.isGlobal() && targetIndexes == TargetIndexes.PARTITIONED) {
                continue;
            }
            if (!indexRegistry.isGlobal() && targetIndexes == TargetIndexes.GLOBAL) {
                continue;
            }

            InternalIndex[] indexesSnapshot = indexRegistry.getIndexes();

            IndexRegistry.beginPartitionUpdate(indexesSnapshot);

            CacheDeserializedValues cacheDeserializedValues = mapContainer.getMapConfig().getCacheDeserializedValues();
            CachedQueryEntry<?, ?> cachedEntry = cacheDeserializedValues == NEVER ? new CachedQueryEntry<>(serializationService,
                    mapContainer.getExtractors()) : null;
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

            IndexRegistry.markPartitionAsIndexed(event.getPartitionId(), indexesSnapshot);
        }

        if (logger.isFinestEnabled()) {
            logger.finest("Populated indexes at step `%s`:[%s]", stepName, event);
        }
    }

    private void depopulateIndexes(PartitionMigrationEvent event, String stepName) {
        assert event.getMigrationEndpoint() == SOURCE;
        assert event.getNewReplicaIndex() != 0 : "Invalid migration event: " + event;

        if (event.getCurrentReplicaIndex() != 0) {
            // backup partitions have no indexes to depopulate
            return;
        }

        PartitionContainer container = mapServiceContext.getPartitionContainer(event.getPartitionId());
        for (RecordStore<Record> recordStore : container.getMaps().values()) {
            MapContainer mapContainer = recordStore.getMapContainer();
            IndexRegistry indexRegistry = mapContainer.getOrCreateIndexRegistry(event.getPartitionId());
            if (!indexRegistry.haveAtLeastOneIndex()) {
                // no indexRegistry to work with
                continue;
            }

            InternalIndex[] indexesSnapshot = indexRegistry.getIndexes();

            IndexRegistry.beginPartitionUpdate(indexesSnapshot);

            CachedQueryEntry<?, ?> entry = new CachedQueryEntry<>(serializationService, mapContainer.getExtractors());
            recordStore.beforeOperation();
            try {
                recordStore.forEach((key, record) -> {
                    Object value = Records.getValueOrCachedValue(record, serializationService);
                    entry.init(key, value);
                    indexRegistry.removeEntry(entry, Index.OperationSource.SYSTEM);
                }, false, true);
            } finally {
                recordStore.afterOperation();
            }

            IndexRegistry.markPartitionAsUnindexed(event.getPartitionId(), indexesSnapshot);
        }

        if (logger.isFinestEnabled()) {
            logger.finest("Depopulated indexes at step `%s`:[%s]", stepName, event);
        }
    }

    private enum TargetIndexes {
        GLOBAL, PARTITIONED
    }

    public static boolean isLocalPromotion(PartitionMigrationEvent event) {
        return event.getMigrationEndpoint() == DESTINATION && event.getCurrentReplicaIndex() > 0
                && event.getNewReplicaIndex() == 0;
    }

    protected long getNow() {
        return Clock.currentTimeMillis();
    }
}
