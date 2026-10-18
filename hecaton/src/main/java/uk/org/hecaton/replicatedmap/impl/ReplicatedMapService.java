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

package uk.org.hecaton.replicatedmap.impl;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.cluster.MemberSelector;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.config.ReplicatedMapConfig;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.core.EntryListener;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.metrics.DynamicMetricsProvider;
import uk.org.hecaton.internal.metrics.MetricDescriptor;
import uk.org.hecaton.internal.metrics.MetricsCollectionContext;
import uk.org.hecaton.internal.monitor.impl.LocalReplicatedMapStatsImpl;
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.nio.ClassLoaderUtil;
import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.partition.MigrationAwareService;
import uk.org.hecaton.internal.partition.PartitionMigrationEvent;
import uk.org.hecaton.internal.partition.PartitionReplicationEvent;
import uk.org.hecaton.internal.partition.impl.InternalPartitionServiceImpl;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.services.ManagedService;
import uk.org.hecaton.internal.services.RemoteService;
import uk.org.hecaton.internal.services.SplitBrainHandlerService;
import uk.org.hecaton.internal.services.SplitBrainProtectionAwareService;
import uk.org.hecaton.internal.services.StatisticsAwareService;
import uk.org.hecaton.internal.util.ConstructorFunction;
import uk.org.hecaton.internal.util.ContextMutexFactory;
import uk.org.hecaton.replicatedmap.LocalReplicatedMapStats;
import uk.org.hecaton.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;
import uk.org.hecaton.replicatedmap.impl.iterator.ReplicatedMapIterationService;
import uk.org.hecaton.replicatedmap.impl.operation.CheckReplicaVersionOperation;
import uk.org.hecaton.replicatedmap.impl.operation.ReplicationOperation;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedRecordStore;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.eventservice.EventPublishingService;
import uk.org.hecaton.spi.impl.eventservice.impl.TrueEventFilter;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicyProvider;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.splitbrainprotection.SplitBrainProtectionOn;
import uk.org.hecaton.splitbrainprotection.SplitBrainProtectionService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static uk.org.hecaton.internal.config.ConfigValidator.checkReplicatedMapConfig;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_PREFIX;
import static uk.org.hecaton.internal.metrics.impl.ProviderHelper.provide;
import static uk.org.hecaton.internal.util.ConcurrencyUtil.getOrPutSynchronized;
import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;

/**
 * This is the main service implementation to handle proxy creation, event publishing, migration, anti-entropy and
 * manages the backing {@link PartitionContainer}s that actually hold the data
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public class ReplicatedMapService implements ManagedService, RemoteService, EventPublishingService<Object, Object>,
                                             MigrationAwareService, SplitBrainHandlerService,
                                             StatisticsAwareService<LocalReplicatedMapStats>,
                                             SplitBrainProtectionAwareService, DynamicMetricsProvider {
    public static final String SERVICE_NAME = "hz:impl:replicatedMapService";
    public static final int INVOCATION_TRY_COUNT = 3;

    private static final int SYNC_INTERVAL_SECONDS = 30;

    private static final Object NULL_OBJECT = new Object();

    private final AntiEntropyTask antiEntropyTask = new AntiEntropyTask();

    private final ConcurrentMap<String, Object> splitBrainProtectionConfigCache = new ConcurrentHashMap<>();
    private final ContextMutexFactory splitBrainProtectionConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> splitBrainProtectionConfigConstructor =
            new ConstructorFunction<>() {
        @Override
        public Object createNew(String name) {
            ReplicatedMapConfig lockConfig = nodeEngine.getConfig().findReplicatedMapConfig(name);
            String splitBrainProtectionName = lockConfig.getSplitBrainProtectionName();
            return splitBrainProtectionName == null ? NULL_OBJECT : splitBrainProtectionName;
        }
    };

    private final Config config;
    private final NodeEngine nodeEngine;
    private final PartitionContainer[] partitionContainers;
    private final InternalPartitionServiceImpl partitionService;
    private final ClusterService clusterService;
    private final OperationService operationService;
    private final SplitBrainProtectionService splitBrainProtectionService;
    private final ReplicatedMapEventPublishingService eventPublishingService;
    private final ReplicatedMapSplitBrainHandlerService splitBrainHandlerService;
    private final ReplicatedMapIterationService iterationService;
    private final LocalReplicatedMapStatsProvider statsProvider;
    private final SplitBrainMergePolicyProvider mergePolicyProvider;

    private ScheduledFuture antiEntropyFuture;

    public ReplicatedMapService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.config = nodeEngine.getConfig();
        this.partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        this.clusterService = nodeEngine.getClusterService();
        this.operationService = nodeEngine.getOperationService();
        this.partitionContainers = new PartitionContainer[nodeEngine.getPartitionService().getPartitionCount()];
        this.eventPublishingService = new ReplicatedMapEventPublishingService(this);
        this.splitBrainHandlerService = new ReplicatedMapSplitBrainHandlerService(this);
        this.iterationService = new ReplicatedMapIterationService(this, nodeEngine.getSerializationService(), nodeEngine);
        this.splitBrainProtectionService = nodeEngine.getSplitBrainProtectionService();
        this.mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        this.statsProvider = new LocalReplicatedMapStatsProvider(config, partitionContainers);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            partitionContainers[i] = new PartitionContainer(this, i);
        }
        antiEntropyFuture = nodeEngine.getExecutionService().getGlobalTaskScheduler()
                .scheduleWithRepetition(antiEntropyTask, 0, SYNC_INTERVAL_SECONDS, TimeUnit.SECONDS);

        boolean dsMetricsEnabled = nodeEngine.getProperties().getBoolean(ClusterProperty.METRICS_DATASTRUCTURES);
        if (dsMetricsEnabled) {
            nodeEngine.getMetricsRegistry().registerDynamicMetricsProvider(this);
        }
    }

    @Override
    public void reset() {
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            ConcurrentMap<String, ReplicatedRecordStore> stores = partitionContainers[i].getStores();
            for (ReplicatedRecordStore store : stores.values()) {
                store.reset();
            }
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        for (PartitionContainer container : partitionContainers) {
            if (container != null) {
                container.shutdown();
            }
        }
        if (antiEntropyFuture != null) {
            antiEntropyFuture.cancel(true);
        }
        this.iterationService.shutdown();
    }

    /**
     * Gets the {@link LocalReplicatedMapStatsImpl} implementation of {@link LocalReplicatedMapStats} for the provided
     * {@code name} of the replicated map. This is used for operations that mutate replicated map's local statistics.
     *
     * @param name of the replicated map.
     * @return replicated map's local statistics object.
     */
    public LocalReplicatedMapStatsImpl getLocalReplicatedMapStatsImpl(String name) {
        return statsProvider.getLocalReplicatedMapStatsImpl(name);
    }

    /**
     * Gets the replicated map's local statistics. If the statistics is disabled so method returns always the same object which is
     * empty and immutable.
     *
     * @param name of the replicated map.
     * @return replicated map's local statistics object.
     */
    public LocalReplicatedMapStats getLocalReplicatedMapStats(String name) {
        return statsProvider.getLocalReplicatedMapStats(name);
    }

    @Override
    public DistributedObject createDistributedObject(String objectName, UUID source, boolean local) {
        ReplicatedMapConfig replicatedMapConfig = getReplicatedMapConfig(objectName);
        checkReplicatedMapConfig(replicatedMapConfig, mergePolicyProvider);
        if (nodeEngine.getLocalMember().isLiteMember()) {
            throw new ReplicatedMapCantBeCreatedOnLiteMemberException(nodeEngine.getThisAddress());
        }

        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            PartitionContainer partitionContainer = partitionContainers[i];
            if (partitionContainer == null) {
                continue;
            }
            partitionContainer.getOrCreateRecordStore(objectName);
        }
        return new ReplicatedMapProxy(nodeEngine, objectName, this, replicatedMapConfig);
    }

    @Override
    public void destroyDistributedObject(String objectName, boolean local) {
        if (nodeEngine.getLocalMember().isLiteMember()) {
            return;
        }

        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            partitionContainers[i].destroy(objectName);
        }
        splitBrainProtectionConfigCache.remove(objectName);
    }

    @Override
    public void dispatchEvent(Object event, Object listener) {
        eventPublishingService.dispatchEvent(event, listener);
    }

    public ReplicatedMapConfig getReplicatedMapConfig(String name) {
        return config.findReplicatedMapConfig(name);
    }

    public ReplicatedRecordStore getReplicatedRecordStore(String name, boolean create, Object key) {
        return getReplicatedRecordStore(name, create, partitionService.getPartitionId(key));
    }

    public ReplicatedRecordStore getReplicatedRecordStore(String name, boolean create, int partitionId) {
        if (nodeEngine.getLocalMember().isLiteMember()) {
            throw new ReplicatedMapCantBeCreatedOnLiteMemberException(nodeEngine.getThisAddress());
        }

        PartitionContainer partitionContainer = partitionContainers[partitionId];
        if (create) {
            return partitionContainer.getOrCreateRecordStore(name);
        }
        return partitionContainer.getRecordStore(name);
    }

    public Collection<ReplicatedRecordStore> getAllReplicatedRecordStores(String name) {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        ArrayList<ReplicatedRecordStore> stores = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            PartitionContainer partitionContainer = partitionContainers[i];
            if (partitionContainer == null) {
                continue;
            }
            ReplicatedRecordStore recordStore = partitionContainer.getRecordStore(name);
            if (recordStore == null) {
                continue;
            }
            stores.add(recordStore);
        }
        return stores;
    }

    private Collection<Address> getMemberAddresses(MemberSelector memberSelector) {
        Collection<Member> members = clusterService.getMembers(memberSelector);
        Collection<Address> addresses = new ArrayList<>(members.size());
        for (Member member : members) {
            addresses.add(member.getAddress());
        }
        return addresses;
    }

    public void initializeListeners(String name) {
        ReplicatedMapConfig mapConfig = getReplicatedMapConfig(name);
        List<ListenerConfig> listenerConfigs = mapConfig.getListenerConfigs();
        for (ListenerConfig listenerConfig : listenerConfigs) {
            EntryListener listener = null;
            if (listenerConfig.getImplementation() != null) {
                listener = (EntryListener) listenerConfig.getImplementation();
            } else if (listenerConfig.getClassName() != null) {
                try {
                    ClassLoader loader = NamespaceUtil.getClassLoaderForNamespace(nodeEngine, mapConfig.getUserCodeNamespace());
                    listener = ClassLoaderUtil.newInstance(loader, listenerConfig.getClassName());
                } catch (Exception e) {
                    throw rethrow(e);
                }
            }
            if (listener != null) {
                if (listener instanceof HecatonInstanceAware aware) {
                    aware.setHecatonInstance(nodeEngine.getHecatonInstance());
                }
                eventPublishingService.addLocalEventListener(listener, TrueEventFilter.INSTANCE, name);
            }
        }
    }

    public PartitionContainer getPartitionContainer(int partitionId) {
        return partitionContainers[partitionId];
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public ReplicatedMapEventPublishingService getEventPublishingService() {
        return eventPublishingService;
    }

    public ReplicatedMapIterationService getIterationService() {
        return iterationService;
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (nodeEngine.getLocalMember().isLiteMember()) {
            return null;
        }
        if (event.getReplicaIndex() > 0) {
            return null;
        }

        PartitionContainer container = partitionContainers[event.getPartitionId()];
        SerializationService serializationService = nodeEngine.getSerializationService();
        ReplicationOperation operation = new ReplicationOperation(serializationService, container, event.getPartitionId());
        operation.setService(this);
        return operation.isEmpty() ? null : operation;
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        // no-op
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        // no-op
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        // no-op
    }

    @Override
    public Runnable prepareMergeRunnable() {
        return splitBrainHandlerService.prepareMergeRunnable();
    }

    @Override
    public Map<String, LocalReplicatedMapStats> getStats() {
        Collection<String> maps = getNodeEngine().getProxyService().getDistributedObjectNames(SERVICE_NAME);
        Map<String, LocalReplicatedMapStats> mapStats = new HashMap<>(maps.size());
        for (String map : maps) {
            mapStats.put(map, getLocalReplicatedMapStats(map));
        }
        return mapStats;
    }

    @Override
    public String getSplitBrainProtectionName(String name) {
        Object splitBrainProtectionName = getOrPutSynchronized(splitBrainProtectionConfigCache, name,
                splitBrainProtectionConfigCacheMutexFactory, splitBrainProtectionConfigConstructor);
        return splitBrainProtectionName == NULL_OBJECT ? null : (String) splitBrainProtectionName;
    }

    public void ensureNoSplitBrain(String distributedObjectName,
                                   SplitBrainProtectionOn requiredSplitBrainProtectionPermissionType) {
        splitBrainProtectionService.ensureNoSplitBrain(getSplitBrainProtectionName(distributedObjectName),
                requiredSplitBrainProtectionPermissionType);
    }

    // needed for a test
    public void triggerAntiEntropy() {
        antiEntropyTask.triggerAntiEntropy();
    }

    public Object getMergePolicy(String name) {
        var replicatedMapConfig = getReplicatedMapConfig(name);
        return mergePolicyProvider.getMergePolicy(
                replicatedMapConfig.getMergePolicyConfig().getPolicy(),
                replicatedMapConfig.getUserCodeNamespace()
        );
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        provide(descriptor, context, REPLICATED_MAP_PREFIX, getStats());
    }

    /**
     * Looks up the User Code Namespace name associated with the specified replicated map name. This is done
     * by checking the Node's config tree directly.
     *
     * @param engine  {@link NodeEngine} implementation of this member for service and config lookups
     * @param mapName The name of the {@link uk.org.hecaton.replicatedmap.ReplicatedMap} to lookup for
     * @return the Namespace Name if found, or {@code null} otherwise.
     */
    public static String lookupNamespace(NodeEngine engine, String mapName) {
        if (engine.getNamespaceService().isEnabled()) {
            // No regular containers available, fallback to config
            ReplicatedMapConfig config = engine.getConfig().findReplicatedMapConfig(mapName);
            if (config != null) {
                return config.getUserCodeNamespace();
            }
        }
        return null;
    }

    private class AntiEntropyTask implements Runnable {

        @Override
        public void run() {
            triggerAntiEntropy();
        }

        /**
         * Sends an operation to all replicas to check their replica versions for all partitions for which this node is the owner.
         */
        void triggerAntiEntropy() {
            if (nodeEngine.getLocalMember().isLiteMember() || clusterService.getSize(DATA_MEMBER_SELECTOR) == 1) {
                return;
            }
            Collection<Address> addresses = new ArrayList<>(getMemberAddresses(DATA_MEMBER_SELECTOR));
            addresses.remove(nodeEngine.getThisAddress());
            for (int i = 0; i < partitionContainers.length; i++) {
                Address thisAddress = nodeEngine.getThisAddress();
                InternalPartition partition = partitionService.getPartition(i, false);
                Address ownerAddress = partition.getOwnerOrNull();
                if (!thisAddress.equals(ownerAddress)) {
                    continue;
                }
                PartitionContainer partitionContainer = partitionContainers[i];
                if (partitionContainer.isEmpty()) {
                    continue;
                }
                for (Address address : addresses) {
                    Operation operation = new CheckReplicaVersionOperation(partitionContainer)
                            .setPartitionId(i)
                            .setValidateTarget(false);
                    operationService.createInvocationBuilder(SERVICE_NAME, operation, address)
                            .setTryCount(INVOCATION_TRY_COUNT)
                            .invoke();
                }
            }
        }
    }
}
