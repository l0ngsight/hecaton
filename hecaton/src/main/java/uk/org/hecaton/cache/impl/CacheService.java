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

package uk.org.hecaton.cache.impl;

import uk.org.hecaton.cache.impl.event.CacheWanEventPublisher;
import uk.org.hecaton.cache.impl.operation.CacheMergeOperation;
import uk.org.hecaton.cache.impl.operation.CacheRemoveOperation;
import uk.org.hecaton.cache.impl.operation.CacheReplicationOperation;
import uk.org.hecaton.cache.impl.operation.MutableOperation;
import uk.org.hecaton.cache.impl.wan.CacheWanEventPublisherImpl;
import uk.org.hecaton.cache.impl.wan.WanCacheRemoveEvent;
import uk.org.hecaton.cache.impl.wan.WanCacheUpdateEvent;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.config.CacheSimpleConfig;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.WanAcknowledgeType;
import uk.org.hecaton.config.WanReplicationRef;
import uk.org.hecaton.instance.module.NativeMemoryCapability;
import uk.org.hecaton.internal.nearcache.impl.invalidation.MetaDataGenerator;
import uk.org.hecaton.internal.partition.MigrationAwareService;
import uk.org.hecaton.internal.partition.PartitionMigrationEvent;
import uk.org.hecaton.internal.partition.PartitionReplicationEvent;
import uk.org.hecaton.internal.services.DistributedObjectNamespace;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.internal.services.WanSupportingService;
import uk.org.hecaton.internal.util.HecatonFeature;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.merge.MergingValueFactory;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;
import uk.org.hecaton.wan.WanEventCounters;
import uk.org.hecaton.wan.impl.DelegatingWanScheme;
import uk.org.hecaton.wan.impl.InternalWanEvent;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static uk.org.hecaton.internal.partition.MigrationEndpoint.DESTINATION;
import static uk.org.hecaton.internal.partition.MigrationEndpoint.SOURCE;

/**
 * Cache Service is the main access point of JCache implementation.
 * <p>
 * This service is responsible for:
 * <ul>
 * <li>Creating and/or accessing the named {@link uk.org.hecaton.cache.impl.CacheRecordStore}.</li>
 * <li>Creating/Deleting the cache configuration of the named {@link uk.org.hecaton.cache.ICache}.</li>
 * <li>Registering/Deregistering of cache listeners.</li>
 * <li>Publish/dispatch cache events.</li>
 * <li>Enabling/Disabling statistic and management.</li>
 * <li>Data migration commit/rollback through {@link MigrationAwareService}.</li>
 * </ul>
 * <p><b>WARNING:</b>This service is an optionally registered service which is enabled when JCache
 * is located on the classpath, as determined by {@link JCacheDetector#isJCacheAvailable(ClassLoader)}.</p>
 * <p>
 * If registered, it will provide all the above cache operations for all partitions of the node which it
 * is registered on.
 * </p>
 * <p><b>Distributed Cache Name</b> is used for providing a unique name to a cache object to overcome cache manager
 * scoping which depends on URI and class loader parameters. It's a simple concatenation of CacheNamePrefix and
 * cache name where CacheNamePrefix is calculated by each cache manager
 * using {@code AbstractHecatonCacheManager#getCacheNamePrefix()}.
 * </p>
 */
public class CacheService extends AbstractCacheService implements WanSupportingService {

    private volatile CacheWanEventPublisher wanEventPublisher;

    @Override
    protected CachePartitionSegment newPartitionSegment(int partitionId) {
        return new CachePartitionSegment(this, partitionId);
    }

    @Override
    protected ICacheRecordStore createNewRecordStore(String cacheNameWithPrefix, int partitionId) {
        CacheConfig cacheConfig = getCacheConfig(cacheNameWithPrefix);
        if (cacheConfig != null && cacheConfig.getInMemoryFormat() == InMemoryFormat.NATIVE) {
            ICacheRecordStore recordStore = ((NodeEngineImpl) nodeEngine).getNode().getMemberModules()
                    .require(NativeMemoryCapability.class, "NATIVE in-memory format")
                    .createCacheRecordStore(cacheNameWithPrefix, partitionId, nodeEngine, this);
            if (recordStore instanceof AbstractCacheRecordStore<?, ?> abstractRecordStore) {
                abstractRecordStore.instrument(nodeEngine);
            }
            return recordStore;
        }
        CacheRecordStore recordStore = new CacheRecordStore(cacheNameWithPrefix, partitionId, nodeEngine, this);
        recordStore.instrument(nodeEngine);
        return recordStore;
    }

    @Override
    protected CacheOperationProvider createOperationProvider(String nameWithPrefix, InMemoryFormat inMemoryFormat) {
        return new DefaultOperationProvider(nameWithPrefix);
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        CachePartitionSegment segment = segments[event.getPartitionId()];
        return segment.getAllNamespaces(event.getReplicaIndex());
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        return namespace instanceof ObjectNamespace && SERVICE_NAME.equals(namespace.getServiceName());
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        CachePartitionSegment segment = segments[event.getPartitionId()];
        return prepareReplicationOperation(event, segment.getAllNamespaces(event.getReplicaIndex()));
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                 Collection<ServiceNamespace> namespaces) {
        assert assertAllKnownNamespaces(namespaces);

        CachePartitionSegment segment = segments[event.getPartitionId()];
        CacheReplicationOperation op = newCacheReplicationOperation();
        op.setPartitionId(event.getPartitionId());
        op.prepare(segment, namespaces, event.getReplicaIndex());
        return op.isEmpty() ? null : op;
    }

    protected boolean assertAllKnownNamespaces(Collection<ServiceNamespace> namespaces) {
        for (ServiceNamespace namespace : namespaces) {
            assert isKnownServiceNamespace(namespace) : namespace + " is not a CacheService namespace!";
        }
        return true;
    }

    protected CacheReplicationOperation newCacheReplicationOperation() {
        return new CacheReplicationOperation();
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        super.commitMigration(event);

        if (SOURCE == event.getMigrationEndpoint()) {
            getMetaDataGenerator().removeUuidAndSequence(event.getPartitionId());
        } else if (DESTINATION == event.getMigrationEndpoint()) {
            if (event.getNewReplicaIndex() != 0) {
                getMetaDataGenerator().regenerateUuid(event.getPartitionId());
            }
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        super.rollbackMigration(event);

        if (DESTINATION == event.getMigrationEndpoint()) {
            getMetaDataGenerator().removeUuidAndSequence(event.getPartitionId());
        }
    }

    private MetaDataGenerator getMetaDataGenerator() {
        return cacheEventHandler.getMetaDataGenerator();
    }

    @Override
    public String toString() {
        return "CacheService[" + SERVICE_NAME + ']';
    }

    @Override
    public boolean isWanReplicationEnabled(String cacheNameWithPrefix) {
        CacheConfig config = getCacheConfig(cacheNameWithPrefix);
        WanReplicationRef ref = config == null ? null : config.getWanReplicationRef();
        return ref != null && nodeEngine.getWanReplicationService().hasWanReplicationScheme(ref.getName());
    }

    @Override
    public CacheWanEventPublisher getCacheWanEventPublisher() {
        CacheWanEventPublisher publisher = wanEventPublisher;
        if (publisher == null) {
            publisher = new CacheWanEventPublisherImpl(nodeEngine, this::getCacheConfig);
            wanEventPublisher = publisher;
        }
        return publisher;
    }

    @Override
    public void doPrepublicationChecks(String cacheName) {
        CacheConfig config = getCacheConfig(cacheName);
        WanReplicationRef ref = config == null ? null : config.getWanReplicationRef();
        if (ref != null) {
            DelegatingWanScheme scheme = nodeEngine.getWanReplicationService().getWanReplicationPublishers(ref.getName());
            if (scheme != null) {
                scheme.doPrepublicationChecks();
            }
        }
    }

    // WanSupportingService: applying the cache events received by WAN replication

    @Override
    public void onReplicationEvent(InternalWanEvent event, WanAcknowledgeType acknowledgeType) {
        CompletionStage<Void> applied = applyReplicationEvent(event);
        if (acknowledgeType != WanAcknowledgeType.ACK_ON_RECEIPT) {
            applied.toCompletableFuture().join();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletionStage<Void> applyReplicationEvent(InternalWanEvent event) {
        WanEventCounters counters = nodeEngine.getWanReplicationService().getReceivedEventCounters(SERVICE_NAME);
        String cacheName = event.getObjectName();
        Operation operation;
        if (event instanceof WanCacheUpdateEvent update) {
            CacheConfig config = getCacheConfig(cacheName);
            SplitBrainMergePolicy<Object, CacheMergeTypes<Object, Object>, Object> mergePolicy =
                    nodeEngine.getSplitBrainMergePolicyProvider().getMergePolicy(update.getMergePolicyClassName(),
                            config == null ? null : config.getUserCodeNamespace());
            CacheMergeTypes<Object, Object> entry =
                    MergingValueFactory.createMergingEntry(nodeEngine.getSerializationService(), update.getEntryView());
            operation = new CacheMergeOperation(cacheName, List.of(entry), mergePolicy, true);
        } else if (event instanceof WanCacheRemoveEvent) {
            operation = new CacheRemoveOperation(cacheName, event.getKey(), null, MutableOperation.IGNORE_COMPLETION, true);
        } else {
            return CompletableFuture.completedFuture(null);
        }
        int partitionId = nodeEngine.getPartitionService().getPartitionId(event.getKey());
        return nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, operation, partitionId)
                .thenAccept(response -> event.incrementEventCount(counters));
    }

    @Override
    public CompletionStage<Void> onSyncBatch(Collection<InternalWanEvent> batch, WanAcknowledgeType acknowledgeType) {
        throw new UnsupportedOperationException(HecatonFeature.WAN_SYNC.notAvailableMessage());
    }

    @Override
    public void onWanConfigChange() {
        // whether a cache replicates is looked up for every operation
    }

    /**
     * Checks if the given namespace is referenced by a hot restart enabled
     * cache configuration.
     *
     * @param namespace  the namespace.
     * @return {@code true} if the namespace is referenced by a hot restart
     * enabled data structure, {@code false} otherwise.
     */
    public boolean isNamespaceReferencedWithHotRestart(@Nonnull String namespace)  {
        return nodeEngine.getConfig()
                .getCacheConfigs()
                .values()
                .stream()
                .filter(cacheConfig -> cacheConfig.getDataPersistenceConfig().isEnabled())
                .map(CacheSimpleConfig::getUserCodeNamespace)
                .anyMatch(namespace::equals)
                || getCacheConfigs()
                .stream()
                .filter(cacheConfig -> cacheConfig.getHotRestartConfig().isEnabled())
                .map(CacheConfig::getUserCodeNamespace)
                .anyMatch(namespace::equals);
    }

    public static ObjectNamespace getObjectNamespace(String cacheName) {
        return new DistributedObjectNamespace(SERVICE_NAME, cacheName);
    }

    /**
     * Looks up the User Code Namespace name associated with the specified cache name. This is done
     * by checking the Node's config tree directly.
     *
     * @param engine    {@link NodeEngine} implementation of this member for service and config lookups
     * @param cacheName The name of the {@link uk.org.hecaton.cache.ICache} to lookup for
     * @return the Namespace Name if found, or {@code null} otherwise.
     */
    public static String lookupNamespace(NodeEngine engine, String cacheName) {
        if (engine.getNamespaceService().isEnabled()) {
            // No regular containers available, fallback to config
            CacheService service = engine.getService(CacheService.SERVICE_NAME);
            var config = service.getCacheConfig(cacheName);
            if (config != null) {
                return config.getUserCodeNamespace();
            }
        }
        return null;
    }
}
