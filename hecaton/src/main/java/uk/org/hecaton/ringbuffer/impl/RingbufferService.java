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

package uk.org.hecaton.ringbuffer.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.RingbufferConfig;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.internal.partition.ChunkedMigrationAwareService;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.internal.partition.PartitionMigrationEvent;
import uk.org.hecaton.internal.partition.PartitionReplicationEvent;
import uk.org.hecaton.internal.partition.impl.NameSpaceUtil;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.services.DistributedObjectNamespace;
import uk.org.hecaton.internal.services.ManagedService;
import uk.org.hecaton.internal.services.ObjectNamespace;
import uk.org.hecaton.internal.services.RemoteService;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.internal.services.SplitBrainHandlerService;
import uk.org.hecaton.internal.services.SplitBrainProtectionAwareService;
import uk.org.hecaton.internal.util.ConstructorFunction;
import uk.org.hecaton.internal.util.ContextMutexFactory;
import uk.org.hecaton.partition.strategy.StringPartitioningStrategy;
import uk.org.hecaton.ringbuffer.impl.operations.MergeOperation;
import uk.org.hecaton.ringbuffer.impl.operations.ReplicationOperation;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.merge.AbstractContainerMerger;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.merge.RingbufferMergeData;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.RingbufferMergeTypes;
import uk.org.hecaton.splitbrainprotection.SplitBrainProtectionOn;
import uk.org.hecaton.splitbrainprotection.SplitBrainProtectionService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static uk.org.hecaton.internal.config.ConfigValidator.checkRingbufferConfig;
import static uk.org.hecaton.internal.partition.MigrationEndpoint.DESTINATION;
import static uk.org.hecaton.internal.partition.MigrationEndpoint.SOURCE;
import static uk.org.hecaton.internal.util.ConcurrencyUtil.getOrPutSynchronized;
import static uk.org.hecaton.internal.util.MapUtil.isNullOrEmpty;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;

/**
 * The SPI Service that deals with the {@link uk.org.hecaton.ringbuffer.Ringbuffer}.
 */
public class RingbufferService implements ManagedService, RemoteService, ChunkedMigrationAwareService,
        SplitBrainProtectionAwareService, SplitBrainHandlerService {

    /**
     * Prefix of ringbuffers that are created for topics. Using a prefix prevents
     * users accidentally retrieving the ringbuffer.
     */
    public static final String TOPIC_RB_PREFIX = "_hz_rb_";

    /**
     * The ringbuffer service name which defines it in the node engine.
     */
    public static final String SERVICE_NAME = "hz:impl:ringbufferService";

    private static final Object NULL_OBJECT = new Object();

    /**
     * Map from namespace to actual ringbuffer containers. The namespace
     * defines the service and object name which is the owner of the ringbuffer
     * container.
     */
    private final ConcurrentMap<Integer, Map<ObjectNamespace, RingbufferContainer>> containers
            = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Object> splitBrainProtectionConfigCache = new ConcurrentHashMap<>();
    private final ContextMutexFactory splitBrainProtectionConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> splitBrainProtectionConfigConstructor =
            new ConstructorFunction<>() {
                @Override
                public Object createNew(String name) {
                    RingbufferConfig config = nodeEngine.getConfig().findRingbufferConfig(name);
                    String splitBrainProtectionName = config.getSplitBrainProtectionName();
                    // The splitBrainProtectionName will be null
                    // if there is no split brain protection
                    // defined for this data structure, but the
                    // SplitBrainProtectionService is active,
                    // due to another data structure with a
                    // split brain protection configuration
                    return splitBrainProtectionName == null ? NULL_OBJECT : splitBrainProtectionName;
                }
            };

    private NodeEngine nodeEngine;
    private SerializationService serializationService;
    private IPartitionService partitionService;
    private SplitBrainProtectionService splitBrainProtectionService;

    public RingbufferService(NodeEngineImpl nodeEngine) {
        init(nodeEngine, null);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = checkNotNull(nodeEngine, "nodeEngine can't be null");
        this.serializationService = nodeEngine.getSerializationService();
        this.partitionService = nodeEngine.getPartitionService();
        this.splitBrainProtectionService = nodeEngine.getSplitBrainProtectionService();
    }

    // just for testing
    public ConcurrentMap<Integer, Map<ObjectNamespace, RingbufferContainer>> getContainers() {
        return containers;
    }

    @Override
    public DistributedObject createDistributedObject(String objectName, UUID source, boolean local) {
        RingbufferConfig ringbufferConfig = getRingbufferConfig(objectName);
        checkRingbufferConfig(ringbufferConfig, nodeEngine.getSplitBrainMergePolicyProvider());

        return new RingbufferProxy(nodeEngine, this, objectName, ringbufferConfig);
    }

    @Override
    public void destroyDistributedObject(String name, boolean local) {
        destroyContainer(getRingbufferPartitionId(name), getRingbufferNamespace(name));
        nodeEngine.getEventService().deregisterAllLocalListeners(SERVICE_NAME, name);
        splitBrainProtectionConfigCache.remove(name);
    }

    public void destroyContainer(int partitionId, ObjectNamespace namespace) {
        final Map<ObjectNamespace, RingbufferContainer> partitionContainers = containers.get(partitionId);
        if (partitionContainers == null) {
            return;
        }
        partitionContainers.remove(namespace);
    }

    @Override
    public void reset() {
        containers.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    /**
     * Return the ringbuffer containter for the specified {@code namespace}.
     * If there is no ringbuffer container, create it using the {@code config}.
     *
     * @param namespace the ringbuffer container namespace
     * @param config    the ringbuffer config. Used to create the container when the container doesn't exist
     * @return the ringbuffer container
     * @throws NullPointerException if the {@code config} is {@code null}
     */
    @SuppressWarnings("unchecked")
    public <T, E> RingbufferContainer<T, E> getOrCreateContainer(int partitionId, ObjectNamespace namespace,
                                                                 RingbufferConfig config) {
        if (config == null) {
            throw new NullPointerException("Ringbuffer config must not be null when ringbuffer is being created");
        }
        final Map<ObjectNamespace, RingbufferContainer> partitionContainers = getOrCreateRingbufferContainers(partitionId);

        RingbufferContainer<T, E> ringbuffer = partitionContainers.get(namespace);
        if (ringbuffer != null) {
            return ringbuffer;
        }

        ringbuffer = new RingbufferContainer<>(namespace, config, nodeEngine, partitionId);
        ringbuffer.getStore().instrument(nodeEngine);
        partitionContainers.put(namespace, ringbuffer);
        return ringbuffer;
    }

    /**
     * Returns the ringbuffer container if it already exists for the
     * given {@code partitionId} and {@code namespace}. Returns {@code null}
     * if it doesn't exist.
     *
     * @param partitionId the partition ID of the ringbuffer container
     * @param namespace   the namespace of the ringbuffer container
     * @param <T>         the type of items in the ringbuffer container
     * @param <E>         the type of items in the ringbuffer
     * @return the ringbuffer container or {@code null} if it has not been created
     */
    @SuppressWarnings("unchecked")
    public <T, E> RingbufferContainer<T, E> getContainerOrNull(int partitionId, ObjectNamespace namespace) {
        final Map<ObjectNamespace, RingbufferContainer> partitionContainers = containers.get(partitionId);
        return partitionContainers != null ? partitionContainers.get(namespace) : null;
    }

    private Map<ObjectNamespace, RingbufferContainer> getOrCreateRingbufferContainers(int partitionId) {
        final Map<ObjectNamespace, RingbufferContainer> partitionContainer = containers.get(partitionId);
        if (partitionContainer == null) {
            containers.putIfAbsent(partitionId, new ConcurrentHashMap<>());
        }
        return containers.get(partitionId);
    }

    public RingbufferConfig getRingbufferConfig(String name) {
        Config config = nodeEngine.getConfig();
        return config.findRingbufferConfig(getConfigName(name));
    }

    public static ObjectNamespace getRingbufferNamespace(String name) {
        return new DistributedObjectNamespace(SERVICE_NAME, name);
    }

    public int getRingbufferPartitionId(String ringbufferName) {
        Data partitionAwareData = serializationService.toData(ringbufferName, StringPartitioningStrategy.INSTANCE);
        return partitionService.getPartitionId(partitionAwareData);
    }

    public void addRingbuffer(int partitionId, RingbufferContainer ringbuffer, RingbufferConfig config) {
        checkNotNull(ringbuffer, "ringbuffer can't be null");
        ringbuffer.init(config, nodeEngine);
        ringbuffer.getStore().instrument(nodeEngine);
        getOrCreateRingbufferContainers(partitionId).put(ringbuffer.getNamespace(), ringbuffer);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return prepareReplicationOperation(event, getAllServiceNamespaces(event));
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                 Collection<ServiceNamespace> namespaces) {
        int partitionId = event.getPartitionId();
        Map<ObjectNamespace, RingbufferContainer> partitionContainers = containers.get(partitionId);
        if (isNullOrEmpty(partitionContainers)) {
            return null;
        }

        Map<ObjectNamespace, RingbufferContainer> migrationData = new HashMap<>();
        for (ServiceNamespace namespace : namespaces) {
            ObjectNamespace ns = (ObjectNamespace) namespace;
            RingbufferContainer container = partitionContainers.get(ns);
            if (container != null && container.getConfig().getTotalBackupCount() >= event.getReplicaIndex()) {
                migrationData.put(ns, container);
            }
        }
        if (migrationData.isEmpty()) {
            return null;
        }
        return new ReplicationOperation(migrationData, event.getPartitionId(), event.getReplicaIndex());
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == SOURCE) {
            clearRingbuffersHavingLesserBackupCountThan(event.getPartitionId(), event.getNewReplicaIndex());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == DESTINATION) {
            clearRingbuffersHavingLesserBackupCountThan(event.getPartitionId(), event.getCurrentReplicaIndex());
        }
    }

    private void clearRingbuffersHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        final Map<ObjectNamespace, RingbufferContainer> partitionContainers = containers.get(partitionId);
        if (partitionContainers == null || partitionContainers.isEmpty()) {
            return;
        }

        final Iterator<Entry<ObjectNamespace, RingbufferContainer>> iterator = partitionContainers.entrySet().iterator();
        while (iterator.hasNext()) {
            final Entry<ObjectNamespace, RingbufferContainer> entry = iterator.next();
            final RingbufferContainer container = entry.getValue();
            if (thresholdReplicaIndex < 0 || container.getConfig().getTotalBackupCount() < thresholdReplicaIndex) {
                iterator.remove();
            }
        }
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        int partitionId = event.getPartitionId();
        Map<ObjectNamespace, RingbufferContainer> partitionContainers = containers.get(partitionId);

        return NameSpaceUtil.getAllNamespaces(partitionContainers,
                container -> container.getConfig().getTotalBackupCount() >= event.getReplicaIndex(),
                RingbufferContainer::getNamespace);
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        return namespace instanceof ObjectNamespace;
    }

    @Override
    public String getSplitBrainProtectionName(final String name) {
        Object splitBrainProtectionName = getOrPutSynchronized(splitBrainProtectionConfigCache, name,
                splitBrainProtectionConfigCacheMutexFactory, splitBrainProtectionConfigConstructor);
        return splitBrainProtectionName == NULL_OBJECT ? null : (String) splitBrainProtectionName;
    }

    public void ensureNoSplitBrain(String distributedObjectName,
                                   SplitBrainProtectionOn requiredSplitBrainProtectionPermissionType) {
        splitBrainProtectionService.ensureNoSplitBrain(getSplitBrainProtectionName(distributedObjectName),
                requiredSplitBrainProtectionPermissionType);
    }

    @Override
    public Runnable prepareMergeRunnable() {
        RingbufferContainerCollector collector = new RingbufferContainerCollector(nodeEngine, containers);
        collector.run();
        return new Merger(collector);
    }

    private class Merger
            extends AbstractContainerMerger<RingbufferContainer, RingbufferMergeData, RingbufferMergeTypes> {

        Merger(RingbufferContainerCollector collector) {
            super(collector, nodeEngine);
        }

        @Override
        protected String getLabel() {
            return "ringbuffer";
        }

        @Override
        public void runInternal() {
            for (Entry<Integer, Collection<RingbufferContainer>> entry : collector.getCollectedContainers().entrySet()) {
                int partitionId = entry.getKey();
                Collection<RingbufferContainer> containerList = entry.getValue();
                for (RingbufferContainer container : containerList) {
                    // TODO: add batching (which is a bit complex, since collections don't have a multi-name operation yet
                    SplitBrainMergePolicy<RingbufferMergeData, RingbufferMergeTypes, RingbufferMergeData> mergePolicy
                            = getMergePolicy(container.getConfig().getMergePolicyConfig(), container.getUserCodeNamespace());

                    sendBatch(partitionId, mergePolicy, container);
                }
            }
        }

        private void sendBatch(int partitionId,
                               SplitBrainMergePolicy<RingbufferMergeData, RingbufferMergeTypes, RingbufferMergeData> mergePolicy,
                               RingbufferContainer mergingContainer) {
            final MergeOperation operation = new MergeOperation(mergingContainer.getNamespace(), mergePolicy,
                    mergingContainer.getRingbuffer());
            invoke(SERVICE_NAME, operation, partitionId);
        }
    }

    private static String getConfigName(String name) {
        if (name.startsWith(TOPIC_RB_PREFIX)) {
            name = name.substring(TOPIC_RB_PREFIX.length());
        }
        return name;
    }

    /**
     * Looks up the User Code Namespace Name associated with the specified map name. This starts
     * by looking for an existing {@link RingbufferContainer} and checking its defined
     * {@link RingbufferConfig}. If the {@link RingbufferContainer} does not exist (containers
     * are created lazily), then fallback to checking the Node's config tree directly.
     *
     * @param engine         {@link NodeEngine} implementation of this member for service and config lookups
     * @param ringbufferName The name of the {@link uk.org.hecaton.ringbuffer.Ringbuffer} to lookup for
     * @param partitionId    The partition ID for the Ringbuffer defined by the {@code ringbufferName}
     * @return the {@code Namespace} Name if found, or {@code null} otherwise.
     */
    public static String lookupUserCodeNamespace(NodeEngine engine, String ringbufferName, int partitionId) {
        if (engine.getNamespaceService().isEnabled()) {
            RingbufferService service = engine.getService(SERVICE_NAME);
            final ObjectNamespace ns = RingbufferService.getRingbufferNamespace(ringbufferName);
            final RingbufferContainer container = service.getContainerOrNull(partitionId, ns);
            if (container != null) {
                return container.getUserCodeNamespace();
            }
            // Manual config lookup fallback
            RingbufferConfig config = service.getRingbufferConfig(ringbufferName);
            if (config != null) {
                return config.getUserCodeNamespace();
            }
        }
        return null;
    }
}
