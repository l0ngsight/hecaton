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

import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.WanAcknowledgeType;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.internal.cluster.ClusterStateListener;
import uk.org.hecaton.internal.metrics.DynamicMetricsProvider;
import uk.org.hecaton.internal.metrics.MetricDescriptor;
import uk.org.hecaton.internal.metrics.MetricsCollectionContext;
import uk.org.hecaton.internal.partition.ChunkSupplier;
import uk.org.hecaton.internal.partition.ChunkedMigrationAwareService;
import uk.org.hecaton.internal.partition.IPartitionLostEvent;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.internal.partition.OffloadedReplicationPreparation;
import uk.org.hecaton.internal.partition.PartitionAwareService;
import uk.org.hecaton.internal.partition.PartitionMigrationEvent;
import uk.org.hecaton.internal.partition.PartitionReplicationEvent;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.services.ClientAwareService;
import uk.org.hecaton.internal.services.DistributedObjectNamespace;
import uk.org.hecaton.internal.services.LockInterceptorService;
import uk.org.hecaton.internal.services.ManagedService;
import uk.org.hecaton.internal.services.NotifiableEventListener;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.internal.services.PostJoinAwareService;
import uk.org.hecaton.internal.services.RemoteService;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.internal.services.SplitBrainHandlerService;
import uk.org.hecaton.internal.services.SplitBrainProtectionAwareService;
import uk.org.hecaton.internal.services.StatisticsAwareService;
import uk.org.hecaton.internal.services.TenantContextAwareService;
import uk.org.hecaton.internal.services.TransactionalService;
import uk.org.hecaton.internal.services.WanSupportingService;
import uk.org.hecaton.internal.util.MutableLong;
import uk.org.hecaton.map.LocalMapStats;
import uk.org.hecaton.map.impl.event.MapEventPublishingService;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.nearcache.NearCacheStats;
import uk.org.hecaton.query.LocalIndexStats;
import uk.org.hecaton.spi.impl.CountingMigrationAwareService;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.eventservice.EventFilter;
import uk.org.hecaton.spi.impl.eventservice.EventPublishingService;
import uk.org.hecaton.spi.impl.eventservice.EventRegistration;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.transaction.TransactionalObject;
import uk.org.hecaton.transaction.impl.Transaction;
import uk.org.hecaton.wan.impl.InternalWanEvent;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import static uk.org.hecaton.core.EntryEventType.INVALIDATION;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.MAP_DISCRIMINATOR_NAME;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.MAP_METRIC_MAP_STORE_WAITING_TO_BE_PROCESSED_COUNT;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.MAP_PREFIX;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.MAP_PREFIX_ENTRY_PROCESSOR_OFFLOADABLE_EXECUTOR;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.MAP_PREFIX_INDEX;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.MAP_PREFIX_MAP_STORE_OFFLOADED_OPERATIONS;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.MAP_PREFIX_NEARCACHE;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.MAP_TAG_INDEX;

/**
 * Defines map service behavior.
 *
 * @see MapManagedService
 * @see MapMigrationAwareService
 * @see MapTransactionalService
 * @see MapRemoteService
 * @see MapEventPublishingService
 * @see MapPostJoinAwareService
 * @see MapSplitBrainHandlerService
 * @see WanMapSupportingService
 * @see MapPartitionAwareService
 * @see MapSplitBrainProtectionAwareService
 * @see MapClientAwareService
 * @see MapServiceContext
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:MethodCount"})
public class MapService implements ManagedService, ChunkedMigrationAwareService,
        TransactionalService, RemoteService,
        EventPublishingService<Object, ListenerAdapter>,
        PostJoinAwareService, SplitBrainHandlerService,
        WanSupportingService, StatisticsAwareService<LocalMapStats>,
        PartitionAwareService, ClientAwareService,
        SplitBrainProtectionAwareService, NotifiableEventListener,
        ClusterStateListener, LockInterceptorService<Data>,
        DynamicMetricsProvider, TenantContextAwareService,
        OffloadedReplicationPreparation {

    public static final String SERVICE_NAME = "hz:impl:mapService";

    protected ManagedService managedService;
    protected CountingMigrationAwareService migrationAwareService;
    protected TransactionalService transactionalService;
    protected RemoteService remoteService;
    protected EventPublishingService eventPublishingService;
    protected PostJoinAwareService postJoinAwareService;
    protected SplitBrainHandlerService splitBrainHandlerService;
    protected WanSupportingService wanSupportingService;
    protected StatisticsAwareService statisticsAwareService;
    protected PartitionAwareService partitionAwareService;
    protected ClientAwareService clientAwareService;
    protected MapSplitBrainProtectionAwareService splitBrainProtectionAwareService;
    protected MapServiceContext mapServiceContext;

    public MapService() {
    }

    @Override
    public void dispatchEvent(Object event, ListenerAdapter listener) {
        eventPublishingService.dispatchEvent(event, listener);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        managedService.init(nodeEngine, properties);

        boolean dsMetricsEnabled = nodeEngine.getProperties().getBoolean(ClusterProperty.METRICS_DATASTRUCTURES);
        if (dsMetricsEnabled) {
            nodeEngine.getMetricsRegistry().registerDynamicMetricsProvider(this);
        }
    }

    @Override
    public void reset() {
        managedService.reset();
    }

    @Override
    public void shutdown(boolean terminate) {
        managedService.shutdown(terminate);
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        return migrationAwareService.getAllServiceNamespaces(event);
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        return migrationAwareService.isKnownServiceNamespace(namespace);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return migrationAwareService.prepareReplicationOperation(event);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                 Collection<ServiceNamespace> namespaces) {
        return migrationAwareService.prepareReplicationOperation(event, namespaces);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        migrationAwareService.beforeMigration(event);
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        migrationAwareService.commitMigration(event);
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        migrationAwareService.rollbackMigration(event);
    }

    @Override
    public Operation getPostJoinOperation() {
        return postJoinAwareService.getPostJoinOperation();
    }

    @Override
    public DistributedObject createDistributedObject(String objectName, UUID source, boolean local) {
        return remoteService.createDistributedObject(objectName, source, local);
    }

    @Override
    public void destroyDistributedObject(String objectName, boolean local) {
        remoteService.destroyDistributedObject(objectName, local);
        splitBrainProtectionAwareService.onDestroy(objectName);
    }

    @Override
    public void onReplicationEvent(InternalWanEvent event, WanAcknowledgeType acknowledgeType) {
        wanSupportingService.onReplicationEvent(event, acknowledgeType);
    }

    @Override
    public CompletionStage<Void> onSyncBatch(Collection<InternalWanEvent> batch, WanAcknowledgeType acknowledgeType) {
        return wanSupportingService.onSyncBatch(batch, acknowledgeType);
    }

    @Override
    public void onWanConfigChange() {
        wanSupportingService.onWanConfigChange();
    }

    @Override
    public void onPartitionLost(IPartitionLostEvent partitionLostEvent) {
        partitionAwareService.onPartitionLost(partitionLostEvent);
    }

    @Override
    public Runnable prepareMergeRunnable() {
        return splitBrainHandlerService.prepareMergeRunnable();
    }

    @Override
    public <T extends TransactionalObject> T createTransactionalObject(String name, Transaction transaction) {
        return transactionalService.createTransactionalObject(name, transaction);
    }

    @Override
    public void rollbackTransaction(UUID transactionId) {
        transactionalService.rollbackTransaction(transactionId);
    }

    @Override
    public Map<String, LocalMapStats> getStats() {
        return statisticsAwareService.getStats();
    }

    @Override
    public String getSplitBrainProtectionName(String name) {
        return splitBrainProtectionAwareService.getSplitBrainProtectionName(name);
    }

    public MapServiceContext getMapServiceContext() {
        return mapServiceContext;
    }

    @Override
    public void clientDisconnected(UUID clientUuid) {
        clientAwareService.clientDisconnected(clientUuid);
    }

    @Override
    public void onRegister(Object service, String serviceName, String mapName, EventRegistration registration) {
        EventFilter filter = registration.getFilter();
        if (!(filter instanceof EventListenerFilter) || !filter.eval(INVALIDATION.getType())) {
            return;
        }

        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        mapContainer.increaseInvalidationListenerCount();
    }

    @Override
    public void onDeregister(Object service, String serviceName, String mapName, EventRegistration registration) {
        EventFilter filter = registration.getFilter();
        if (!(filter instanceof EventListenerFilter) || !filter.eval(INVALIDATION.getType())) {
            return;
        }

        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        mapContainer.decreaseInvalidationListenerCount();
    }

    public int getMigrationStamp() {
        return migrationAwareService.getMigrationStamp();
    }

    public boolean validateMigrationStamp(int stamp) {
        return migrationAwareService.validateMigrationStamp(stamp);
    }

    @Override
    public void onClusterStateChange(ClusterState newState) {
        mapServiceContext.onClusterStateChange(newState);
    }

    @Override
    public void onBeforeLock(String distributedObjectName, Data key) {
        IPartitionService partitionService = mapServiceContext.getNodeEngine().getPartitionService();
        int partitionId = partitionService.getPartitionId(key);
        RecordStore recordStore = mapServiceContext.getRecordStore(partitionId, distributedObjectName);

        if (recordStore.isTieredStorageEnabled()) {
            // We don't support expiry feature and the getRecordOrNull
            // is done exclusively for this feature support.
            return;
        }
        boolean owner = partitionService.isPartitionOwner(partitionId);
        recordStore.beforeOperation();
        try {
            // we have no use for the return value,
            // invoked just for the side effects
            recordStore.getRecordOrNull(key, !owner);
        } finally {
            recordStore.afterOperation();
        }
    }

    public static ObjectNamespace getObjectNamespace(String mapName) {
        return new DistributedObjectNamespace(SERVICE_NAME, mapName);
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        Map<String, LocalMapStats> stats = getStats();
        if (stats == null) {
            return;
        }

        for (Map.Entry<String, LocalMapStats> entry : stats.entrySet()) {
            String mapName = entry.getKey();
            LocalMapStats localInstanceStats = entry.getValue();

            // map
            MetricDescriptor dsDescriptor = descriptor
                    .copy()
                    .withPrefix(MAP_PREFIX)
                    .withDiscriminator(MAP_DISCRIMINATOR_NAME, mapName);
            context.collect(dsDescriptor, localInstanceStats);

            // index
            Map<String, LocalIndexStats> indexStats = localInstanceStats.getIndexStats();
            for (Map.Entry<String, LocalIndexStats> indexEntry : indexStats.entrySet()) {
                MetricDescriptor indexDescriptor = descriptor
                        .copy()
                        .withPrefix(MAP_PREFIX_INDEX)
                        .withDiscriminator(MAP_DISCRIMINATOR_NAME, mapName)
                        .withTag(MAP_TAG_INDEX, indexEntry.getKey());
                context.collect(indexDescriptor, indexEntry.getValue());
            }

            // near cache
            NearCacheStats nearCacheStats = localInstanceStats.getNearCacheStats();
            if (nearCacheStats != null) {
                MetricDescriptor nearCacheDescriptor = descriptor
                        .copy()
                        .withPrefix(MAP_PREFIX_NEARCACHE)
                        .withDiscriminator(MAP_DISCRIMINATOR_NAME, mapName);
                context.collect(nearCacheDescriptor, nearCacheStats);
            }
        }
        // stats of offloaded-entry-processor's executor
        ExecutorStats executorStats = mapServiceContext.getOffloadedEntryProcessorExecutorStats();
        executorStats.getStatsMap().forEach((name, offloadedExecutorStats) -> {
            MetricDescriptor nearCacheDescriptor = descriptor
                    .copy()
                    .withPrefix(MAP_PREFIX_ENTRY_PROCESSOR_OFFLOADABLE_EXECUTOR)
                    .withDiscriminator(MAP_DISCRIMINATOR_NAME, name);
            context.collect(nearCacheDescriptor, offloadedExecutorStats);
        });

        // mapStore offloaded operations
        setMapStoreOffloadedOperationMetrics(descriptor, context);
    }

    private void setMapStoreOffloadedOperationMetrics(MetricDescriptor descriptor,
                                                      MetricsCollectionContext context) {
        // aggregate
        PartitionContainer[] partitionContainers = mapServiceContext.getPartitionContainers();
        Map<String, MutableLong> offloaded = new HashMap<>();
        for (PartitionContainer partitionContainer : partitionContainers) {
            Collection<RecordStore> allRecordStores = partitionContainer.getAllRecordStores();
            for (RecordStore recordStore : allRecordStores) {
                if (!recordStore.getMapContainer().getMapConfig().isStatisticsEnabled()) {
                    continue;
                }

                MutableLong count = offloaded.computeIfAbsent(recordStore.getName(), s -> new MutableLong());
                count.addAndGet(recordStore.getMapStoreOffloadedOperationsCount());
            }
        }

        // collect metrics
        for (Map.Entry<String, MutableLong> entry : offloaded.entrySet()) {
            String mapName = entry.getKey();
            MutableLong count = entry.getValue();
            MetricDescriptor descriptorOffloaded = descriptor
                    .copy()
                    .withMetric(MAP_METRIC_MAP_STORE_WAITING_TO_BE_PROCESSED_COUNT)
                    .withPrefix(MAP_PREFIX_MAP_STORE_OFFLOADED_OPERATIONS)
                    .withDiscriminator(MAP_DISCRIMINATOR_NAME, mapName);
            context.collect(descriptorOffloaded, count.value);
        }
    }

    @Override
    public boolean shouldOffload() {
        return migrationAwareService.shouldOffload();
    }

    @Override
    public ChunkSupplier newChunkSupplier(PartitionReplicationEvent event, Collection<ServiceNamespace> namespace) {
        return migrationAwareService.newChunkSupplier(event, namespace);
    }

    /**
     * Looks up the User Code Namespace name associated with the specified map name. This starts
     * by looking for an existing {@link MapContainer} and checking its defined
     * {@link MapConfig}. If the {@link MapContainer} does not exist (containers are
     * created lazily), then fallback to checking the Node's config tree directly.
     *
     * @param engine  {@link NodeEngine} implementation of this member for service and config lookups
     * @param mapName The name of the {@link uk.org.hecaton.map.IMap} to lookup for
     * @return the Namespace Name if found, or {@code null} otherwise.
     */
    public static String lookupNamespace(@Nonnull NodeEngine engine, @Nonnull String mapName) {
        if (engine.getNamespaceService().isEnabled()) {
            MapService mapService = engine.getService(MapService.SERVICE_NAME);
            MapContainer container = mapService.getMapServiceContext().getExistingMapContainer(mapName);
            if (container != null) {
                return container.getMapConfig().getUserCodeNamespace();
            }
            // Fallback to config lookup
            MapConfig mapConfig = engine.getConfig().getMapConfigOrNull(mapName);
            if (mapConfig != null) {
                return mapConfig.getUserCodeNamespace();
            }
        }
        return null;
    }

    /**
     * Checks if the given namespace is referenced by a hot restart enabled
     * map configuration.
     *
     * @param engine the node engine.
     * @param namespace  the namespace.
     * @return {@code true} if the namespace is referenced by a hot restart
     * enabled data structure, {@code false} otherwise.
     */
    public static boolean isNamespaceReferencedWithHotRestart(@Nonnull NodeEngine engine, @Nonnull String namespace) {
        return engine
                .getConfig()
                .getMapConfigs()
                .values()
                .stream()
                .filter(cacheConfig -> cacheConfig.getDataPersistenceConfig().isEnabled())
                .map(MapConfig::getUserCodeNamespace)
                .anyMatch(namespace::equals);
    }
}
