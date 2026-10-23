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

package uk.org.hecaton.collection.impl.queue;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.impl.MemberImpl;
import uk.org.hecaton.collection.ItemEvent;
import uk.org.hecaton.collection.ItemListener;
import uk.org.hecaton.collection.LocalQueueStats;
import uk.org.hecaton.collection.impl.common.DataAwareItemEvent;
import uk.org.hecaton.collection.impl.queue.operations.QueueMergeOperation;
import uk.org.hecaton.collection.impl.queue.operations.QueueReplicationOperation;
import uk.org.hecaton.collection.impl.txnqueue.TransactionalQueueProxy;
import uk.org.hecaton.collection.impl.txnqueue.operations.QueueTransactionRollbackOperation;
import uk.org.hecaton.config.QueueConfig;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.core.ItemEventType;
import uk.org.hecaton.internal.loadprotection.LoadProtection;
import uk.org.hecaton.internal.metrics.DynamicMetricsProvider;
import uk.org.hecaton.internal.metrics.MetricDescriptor;
import uk.org.hecaton.internal.metrics.MetricsCollectionContext;
import uk.org.hecaton.internal.monitor.impl.LocalQueueStatsImpl;
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.partition.IPartition;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.internal.partition.MigrationAwareService;
import uk.org.hecaton.internal.partition.MigrationEndpoint;
import uk.org.hecaton.internal.partition.PartitionMigrationEvent;
import uk.org.hecaton.internal.partition.PartitionReplicationEvent;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.services.ManagedService;
import uk.org.hecaton.internal.services.RemoteService;
import uk.org.hecaton.internal.services.SplitBrainHandlerService;
import uk.org.hecaton.internal.services.SplitBrainProtectionAwareService;
import uk.org.hecaton.internal.services.StatisticsAwareService;
import uk.org.hecaton.internal.services.TenantContextAwareService;
import uk.org.hecaton.internal.services.TransactionalService;
import uk.org.hecaton.internal.util.ConcurrencyUtil;
import uk.org.hecaton.internal.util.ConstructorFunction;
import uk.org.hecaton.internal.util.ContextMutexFactory;
import uk.org.hecaton.internal.util.MapUtil;
import uk.org.hecaton.internal.util.scheduler.EntryTaskScheduler;
import uk.org.hecaton.internal.util.scheduler.EntryTaskSchedulerFactory;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.partition.strategy.StringPartitioningStrategy;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.eventservice.EventPublishingService;
import uk.org.hecaton.spi.impl.eventservice.EventRegistration;
import uk.org.hecaton.spi.impl.eventservice.EventService;
import uk.org.hecaton.spi.impl.executionservice.TaskScheduler;
import uk.org.hecaton.spi.impl.merge.AbstractContainerMerger;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.QueueMergeTypes;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.transaction.impl.Transaction;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.internal.config.ConfigValidator.checkQueueConfig;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.QUEUE_PREFIX;
import static uk.org.hecaton.internal.metrics.impl.ProviderHelper.provide;
import static uk.org.hecaton.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static uk.org.hecaton.internal.util.ConcurrencyUtil.getOrPutSynchronized;
import static uk.org.hecaton.internal.util.MapUtil.createHashMap;
import static uk.org.hecaton.internal.util.scheduler.ScheduleType.POSTPONE;
import static uk.org.hecaton.spi.impl.merge.MergingValueFactory.createMergingValue;

/**
 * Provides important services via methods for the the Queue
 * such as {@link uk.org.hecaton.collection.impl.queue.QueueEvictionProcessor }
 */
@SuppressWarnings({"checkstyle:classfanoutcomplexity", "checkstyle:methodcount", "checkstyle:classdataabstractioncoupling"})
public class QueueService implements ManagedService, MigrationAwareService, TransactionalService, RemoteService,
        EventPublishingService<QueueEvent, ItemListener>, StatisticsAwareService<LocalQueueStats>,
        SplitBrainProtectionAwareService, SplitBrainHandlerService, DynamicMetricsProvider,
        TenantContextAwareService {

    public static final String SERVICE_NAME = "hz:impl:queueService";

    private static final Object NULL_OBJECT = new Object();

    private final ConcurrentMap<String, QueueContainer> containerMap = new ConcurrentHashMap<>();
    /** Load protection (F23): the sharded queues this member uses, by logical name. */
    private final ConcurrentMap<String, ShardedQueue> shardedQueues = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, LocalQueueStatsImpl> statsMap;
    private final ConstructorFunction<String, LocalQueueStatsImpl> localQueueStatsConstructorFunction =
            key -> new LocalQueueStatsImpl();

    private final ConcurrentMap<String, Object> splitBrainProtectionConfigCache = new ConcurrentHashMap<>();
    private final ContextMutexFactory splitBrainProtectionConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> splitBrainProtectionConfigConstructor =
            new ConstructorFunction<>() {
                @Override
                public Object createNew(String name) {
                    QueueConfig queueConfig = nodeEngine.getConfig().findQueueConfig(name);
                    String splitBrainProtectionName = queueConfig.getSplitBrainProtectionName();
                    return splitBrainProtectionName == null ? NULL_OBJECT : splitBrainProtectionName;
                }
            };

    private final NodeEngine nodeEngine;
    private final SerializationService serializationService;
    private final IPartitionService partitionService;
    private final ILogger logger;
    private final EntryTaskScheduler<String, Void> queueEvictionScheduler;

    public QueueService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.serializationService = nodeEngine.getSerializationService();
        this.partitionService = nodeEngine.getPartitionService();
        this.logger = nodeEngine.getLogger(QueueService.class);
        TaskScheduler globalScheduler = nodeEngine.getExecutionService().getGlobalTaskScheduler();
        QueueEvictionProcessor entryProcessor = new QueueEvictionProcessor(nodeEngine);
        this.queueEvictionScheduler = EntryTaskSchedulerFactory.newScheduler(globalScheduler, entryProcessor, POSTPONE);
        int approxQueueCount = nodeEngine.getConfig().getQueueConfigs().size();
        this.statsMap = MapUtil.createConcurrentHashMap(approxQueueCount);
    }

    public void scheduleEviction(String name, long delay) {
        queueEvictionScheduler.schedule(delay, name, null);
    }

    public void cancelEviction(String name) {
        queueEvictionScheduler.cancel(name);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        boolean dsMetricsEnabled = nodeEngine.getProperties().getBoolean(ClusterProperty.METRICS_DATASTRUCTURES);
        if (dsMetricsEnabled) {
            nodeEngine.getMetricsRegistry().registerDynamicMetricsProvider(this);
        }
        LoadProtection loadProtection = loadProtection();
        if (loadProtection != null && loadProtection.isEnabled()) {
            nodeEngine.getExecutionService().scheduleWithRepetition(
                    () -> shardedQueues.values().forEach(ShardedQueue::refreshSizeHints),
                    ShardedQueue.REFRESH_MILLIS, ShardedQueue.REFRESH_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * The sharded queue named {@code name}, or {@code null} if the queue has one shard (load
     * protection, F23). Names with a partition key ({@code name@key}), as the shards have, are never
     * sharded.
     */
    public ShardedQueue getShardedQueue(String name) {
        ShardedQueue queue = shardedQueues.get(name);
        if (queue != null) {
            return queue;
        }
        if (name.indexOf('@') >= 0) {
            return null;
        }
        LoadProtection loadProtection = loadProtection();
        QueueConfig config = nodeEngine.getConfig().findQueueConfig(name);
        if (loadProtection == null || loadProtection.shardCount(config) <= 1) {
            return null;
        }
        return shardedQueues.computeIfAbsent(name, n -> new ShardedQueue(n,
                ShardedQueue.shardNames(n, config, partitionService), nodeEngine));
    }

    void forgetShardedQueue(String name) {
        shardedQueues.remove(name);
    }

    private LoadProtection loadProtection() {
        return nodeEngine instanceof NodeEngineImpl impl ? impl.getLoadProtection() : null;
    }

    @Override
    public void reset() {
        containerMap.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    public QueueContainer getOrCreateContainer(final String name, boolean fromBackup) {
        QueueContainer container = containerMap.get(name);
        if (container != null) {
            return container;
        }

        QueueConfig queueConfig = nodeEngine.getConfig().findQueueConfig(name);
        container = new QueueContainer(name, queueConfig, nodeEngine, this);
        QueueContainer existing = containerMap.putIfAbsent(name, container);
        if (existing != null) {
            container = existing;
        } else {
            NamespaceUtil.setupNamespace(nodeEngine, queueConfig.getUserCodeNamespace());
            try {
                container.init(fromBackup);
                container.getStore().instrument(nodeEngine);
            } finally {
                NamespaceUtil.cleanupNamespace(nodeEngine, queueConfig.getUserCodeNamespace());
            }
        }
        return container;
    }

    public QueueContainer getExistingContainerOrNull(String name) {
        QueueContainer container = containerMap.get(name);
        return container != null ? container : null;
    }

    public void addContainer(String name, QueueContainer container) {
        containerMap.put(name, container);
    }

    // need for testing..
    public boolean containsQueue(String name) {
        return containerMap.containsKey(name);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        Map<String, QueueContainer> migrationData = new HashMap<>();
        for (Entry<String, QueueContainer> entry : containerMap.entrySet()) {
            String name = entry.getKey();
            int partitionId = partitionService.getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
            QueueContainer container = entry.getValue();

            if (partitionId == event.getPartitionId()
                    && container.getConfig().getTotalBackupCount() >= event.getReplicaIndex()) {
                if (logger.isFinestEnabled()) {
                    logger.finest(event.toString() + ", " + container.toString());
                }
                migrationData.put(name, container);
            }
        }

        return !migrationData.isEmpty()
                ? new QueueReplicationOperation(migrationData,
                event.getPartitionId(), event.getReplicaIndex()) : null;
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearQueuesHavingLesserBackupCountThan(event.getPartitionId(), event.getNewReplicaIndex());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearQueuesHavingLesserBackupCountThan(event.getPartitionId(), event.getCurrentReplicaIndex());
        }
    }

    private void clearQueuesHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        Iterator<Entry<String, QueueContainer>> iterator = containerMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, QueueContainer> entry = iterator.next();
            String name = entry.getKey();
            QueueContainer container = entry.getValue();
            int containerPartitionId = partitionService.getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
            if (containerPartitionId != partitionId) {
                continue;
            }

            if (thresholdReplicaIndex < 0 || thresholdReplicaIndex > container.getConfig().getTotalBackupCount()) {
                container.destroy();
                iterator.remove();
            }
        }
    }

    @Override
    public void dispatchEvent(QueueEvent event, ItemListener listener) {
        final MemberImpl member = nodeEngine.getClusterService().getMember(event.caller);
        ItemEvent itemEvent = new DataAwareItemEvent(event.name, event.eventType, event.data, member, serializationService);
        if (member == null) {
            if (logger.isInfoEnabled()) {
                logger.info("Dropping event " + itemEvent + " from unknown address:" + event.caller);
            }
            return;
        }

        NamespaceUtil.runWithNamespace(nodeEngine, lookupNamespace(event.getName()), () -> {
            if (event.eventType == ItemEventType.ADDED) {
                listener.itemAdded(itemEvent);
            } else {
                listener.itemRemoved(itemEvent);
            }
        });
        getLocalQueueStatsImpl(event.name).incrementReceivedEvents();
    }

    @Override
    public DistributedObject createDistributedObject(String objectId, UUID source, boolean local) {
        QueueConfig queueConfig = nodeEngine.getConfig().findQueueConfig(objectId);
        checkQueueConfig(queueConfig, nodeEngine.getSplitBrainMergePolicyProvider());

        ShardedQueue sharded = getShardedQueue(objectId);
        if (sharded != null) {
            return new ShardedQueueProxy<>(objectId, sharded, this, nodeEngine);
        }
        return new QueueProxyImpl(objectId, this, nodeEngine, queueConfig);
    }

    @Override
    public void destroyDistributedObject(String name, boolean local) {
        QueueContainer container = containerMap.remove(name);
        if (container != null) {
            container.destroy();
        }
        statsMap.remove(name);
        nodeEngine.getEventService().deregisterAllLocalListeners(SERVICE_NAME, name);
        splitBrainProtectionConfigCache.remove(name);
    }

    public UUID addLocalItemListener(String name, ItemListener listener, boolean includeValue) {
        EventService eventService = nodeEngine.getEventService();
        QueueEventFilter filter = new QueueEventFilter(includeValue);
        return eventService.registerLocalListener(QueueService.SERVICE_NAME, name, filter, listener).getId();
    }

    public UUID addItemListener(String name, ItemListener listener, boolean includeValue) {
        EventService eventService = nodeEngine.getEventService();
        QueueEventFilter filter = new QueueEventFilter(includeValue);
        return eventService.registerListener(QueueService.SERVICE_NAME, name, filter, listener).getId();
    }

    public CompletableFuture<UUID> addItemListenerAsync(String name, ItemListener listener, boolean includeValue) {
        EventService eventService = nodeEngine.getEventService();
        QueueEventFilter filter = new QueueEventFilter(includeValue);
        return eventService.registerListenerAsync(QueueService.SERVICE_NAME, name, filter, listener)
                .thenApplyAsync(EventRegistration::getId, CALLER_RUNS);
    }

    public boolean removeItemListener(String name, UUID registrationId) {
        EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(SERVICE_NAME, name, registrationId);
    }

    public CompletableFuture<Boolean> removeItemListenerAsync(String name, UUID registrationId) {
        EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListenerAsync(SERVICE_NAME, name, registrationId);
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    /**
     * Returns the local queue statistics for the given name and
     * partition ID. If this node is the owner for the partition,
     * returned stats contain {@link LocalQueueStats#getOwnedItemCount()},
     * otherwise it contains {@link LocalQueueStats#getBackupItemCount()}.
     *
     * @param name        the name of the queue for which the statistics are returned
     * @param partitionId the partition ID for which the statistics are returned
     * @return the statistics
     */
    public LocalQueueStats createLocalQueueStats(String name, int partitionId) {
        LocalQueueStatsImpl stats = getLocalQueueStatsImpl(name);
        stats.setOwnedItemCount(0);
        stats.setBackupItemCount(0);
        QueueContainer container = containerMap.get(name);
        if (container == null) {
            return stats;
        }

        Address thisAddress = nodeEngine.getClusterService().getThisAddress();
        IPartition partition = partitionService.getPartition(partitionId, false);

        Address owner = partition.getOwnerOrNull();
        // read-only: this runs outside the partition thread (metrics, getLocalQueueStats())
        if (thisAddress.equals(owner)) {
            stats.setOwnedItemCount(Math.min(container.getConfig().getMaxSize(), container.itemCountForStats()));
        } else if (owner != null) {
            stats.setBackupItemCount(container.itemCountForStats());
        }
        container.setStats(stats);
        return stats;
    }

    /**
     * Returns the local queue statistics for the queue with the given {@code name}. If this node is the owner of the queue,
     * returned stats contain {@link LocalQueueStats#getOwnedItemCount()}, otherwise it contains
     * {@link LocalQueueStats#getBackupItemCount()}.
     *
     * @param name the name of the queue for which the statistics are returned
     * @return the statistics
     */
    public LocalQueueStats createLocalQueueStats(String name) {
        return createLocalQueueStats(name, getPartitionId(name));
    }

    public LocalQueueStatsImpl getLocalQueueStatsImpl(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, localQueueStatsConstructorFunction);
    }

    protected ConcurrentMap<String, LocalQueueStatsImpl> getStatsMap() {
        return statsMap;
    }

    @Override
    public TransactionalQueueProxy createTransactionalObject(String name, Transaction transaction) {
        return new TransactionalQueueProxy(nodeEngine, this, name, transaction);
    }

    @Override
    public void rollbackTransaction(UUID transactionId) {
        final Set<String> queueNames = containerMap.keySet();
        OperationService operationService = nodeEngine.getOperationService();
        for (String name : queueNames) {
            int partitionId = partitionService.getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
            Operation operation = new QueueTransactionRollbackOperation(name, transactionId)
                    .setPartitionId(partitionId)
                    .setService(this)
                    .setNodeEngine(nodeEngine);
            operationService.invokeOnPartition(operation);
        }
    }

    @Override
    public Map<String, LocalQueueStats> getStats() {
        Map<String, LocalQueueStats> queueStats = createHashMap(containerMap.size());
        for (Entry<String, QueueContainer> entry : containerMap.entrySet()) {
            String name = entry.getKey();
            QueueContainer queueContainer = entry.getValue();
            if (queueContainer.getConfig().isStatisticsEnabled()) {
                LocalQueueStats queueStat = createLocalQueueStats(name);
                queueStats.put(name, queueStat);
            }
        }
        return queueStats;
    }

    @Override
    public String getSplitBrainProtectionName(String name) {
        Object splitBrainProtectionName = getOrPutSynchronized(splitBrainProtectionConfigCache, name,
                splitBrainProtectionConfigCacheMutexFactory, splitBrainProtectionConfigConstructor);
        return splitBrainProtectionName == NULL_OBJECT ? null : (String) splitBrainProtectionName;
    }

    @Override
    public Runnable prepareMergeRunnable() {
        QueueContainerCollector collector = new QueueContainerCollector(nodeEngine, containerMap);
        collector.run();
        return new Merger(collector);
    }

    private int getPartitionId(String name) {
        Data keyData = serializationService.toData(name, StringPartitioningStrategy.INSTANCE);
        return partitionService.getPartitionId(keyData);
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        provide(descriptor, context, QUEUE_PREFIX, getStats());
    }

    /**
     * @since 5.1
     */
    public void resetAgeStats(String queueName) {
        QueueContainer queueContainer = containerMap.get(queueName);
        if (queueContainer != null) {
            queueContainer.resetAgeStats();
        }
    }

    /**
     * Looks up the User Code Namespace name associated with the specified queue name. This starts
     * by looking for an existing {@link QueueContainer} and checking its defined
     * {@link QueueConfig}. If the {@link QueueContainer} does not exist (containers are
     * created lazily), then fallback to checking the Node's config tree directly.
     *
     * @param nodeEngine {@link NodeEngine} implementation of this member for service and config lookups
     * @param queueName  The name of the {@link uk.org.hecaton.collection.IQueue} to lookup for
     * @return the Namespace Name if found, or {@code null} otherwise.
     */
    public static String lookupNamespace(NodeEngine nodeEngine, String queueName) {
        if (nodeEngine.getNamespaceService().isEnabled()) {
            QueueService service = nodeEngine.getService(SERVICE_NAME);
            return service.lookupNamespace(queueName);
        }
        return null;
    }

    // For faster access within the service
    private String lookupNamespace(String queueName) {
        QueueContainer container = getExistingContainerOrNull(queueName);
        if (container != null) {
            return container.getConfig().getUserCodeNamespace();
        }
        QueueConfig config = nodeEngine.getConfig().getQueueConfig(queueName);
        if (config != null) {
            return config.getUserCodeNamespace();
        }
        return null;
    }

    private class Merger extends AbstractContainerMerger<QueueContainer, Collection<Object>, QueueMergeTypes<Object>> {

        Merger(QueueContainerCollector collector) {
            super(collector, nodeEngine);
        }

        @Override
        protected String getLabel() {
            return "queue";
        }

        @Override
        public void runInternal() {
            for (Entry<Integer, Collection<QueueContainer>> entry : collector.getCollectedContainers().entrySet()) {
                int partitionId = entry.getKey();
                Collection<QueueContainer> containerList = entry.getValue();
                for (QueueContainer container : containerList) {
                    // TODO: add batching (which is a bit complex, since collections don't have a multi-name operation yet
                    Queue<QueueItem> items = container.getItemQueue();

                    String name = container.getName();
                    var config = container.getConfig();
                    SplitBrainMergePolicy<Collection<Object>, QueueMergeTypes<Object>, Collection<Object>> mergePolicy
                            = getMergePolicy(config.getMergePolicyConfig(), config.getUserCodeNamespace());

                    QueueMergeTypes mergingValue = createMergingValue(serializationService, items);
                    sendBatch(partitionId, name, mergePolicy, mergingValue);
                }
            }
        }

        private void sendBatch(int partitionId, String name,
                               SplitBrainMergePolicy<Collection<Object>, QueueMergeTypes<Object>, Collection<Object>> mergePolicy,
                               QueueMergeTypes mergingValue) {
            QueueMergeOperation operation = new QueueMergeOperation(name, mergePolicy, mergingValue);
            invoke(SERVICE_NAME, operation, partitionId);
        }
    }
}
