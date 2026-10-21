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
package uk.org.hecaton.wan.impl.batch;

import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.AbstractWanPublisherConfig;
import uk.org.hecaton.config.ConsistencyCheckStrategy;
import uk.org.hecaton.config.DiscoveryStrategyConfig;
import uk.org.hecaton.config.WanBatchPublisherConfig;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.instance.ProtocolType;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.config.AliasedDiscoveryConfigUtils;
import uk.org.hecaton.internal.management.ManagementCenterService;
import uk.org.hecaton.internal.monitor.LocalWanPublisherStats;
import uk.org.hecaton.internal.monitor.impl.LocalWanPublisherStatsImpl;
import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.internal.partition.MigrationEndpoint;
import uk.org.hecaton.internal.partition.PartitionMigrationEvent;
import uk.org.hecaton.internal.partition.PartitionReplica;
import uk.org.hecaton.internal.partition.PartitionReplicationEvent;
import uk.org.hecaton.internal.server.ServerConnectionManager;
import uk.org.hecaton.internal.services.DistributedObjectNamespace;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.spi.discovery.integration.DiscoveryService;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.wan.WanEvent;
import uk.org.hecaton.wan.WanEventCounters;
import uk.org.hecaton.wan.WanMigrationAwarePublisher;
import uk.org.hecaton.wan.WanPublisherState;
import uk.org.hecaton.wan.WanQueueFullException;
import uk.org.hecaton.wan.impl.ConsistencyCheckResult;
import uk.org.hecaton.wan.impl.InternalWanEvent;
import uk.org.hecaton.wan.impl.InternalWanPublisher;
import uk.org.hecaton.wan.impl.WanReplicationService;
import uk.org.hecaton.wan.impl.WanReplicationServiceImpl;
import uk.org.hecaton.wan.impl.WanSyncStats;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

/**
 * The WAN batch publisher (F13.2): keeps the WAN events of every partition in a queue, the owner's
 * queue copied to the backups, and sends the events of the partitions the member owns, in
 * batches, to a member of the target cluster.
 * <p>
 * One thread per publisher collects a batch (up to {@code batch-size} events, or what there is
 * after {@code batch-max-delay-millis}), sends it to the current target endpoint and waits for the
 * acknowledgement; only then are the events removed from the queue, and from the backups' copies.
 * One batch at a time keeps the events of a partition in order; a failed batch is sent again,
 * to the next endpoint.
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:MethodCount"})
public class WanBatchPublisher implements InternalWanPublisher<Object>, WanMigrationAwarePublisher<Object> {

    private static final long FAILURE_BACKOFF_MILLIS = 1000;
    private static final String SYNC_EXECUTOR = "hc:wan-sync";

    private final Node node;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final LocalWanPublisherStatsImpl stats = new LocalWanPublisherStatsImpl();

    private String wanReplicationName;
    private String publisherId;
    private WanBatchPublisherConfig config;
    private WanPartitionQueue[] queues;
    private WanTargetEndpoints endpoints;
    private EndpointQualifier endpointQualifier;
    private volatile WanPublisherState state;
    private volatile boolean running;
    private volatile boolean connected;
    private volatile boolean syncFailed;
    private Thread sender;
    private int nextPartition;
    private final Map<UUID, WanSyncTask> syncs = new ConcurrentHashMap<>();
    private final Map<String, WanSyncStats> lastSyncStats = new ConcurrentHashMap<>();
    private final Map<String, ConsistencyCheckResult> lastConsistencyCheckResults = new ConcurrentHashMap<>();

    public WanBatchPublisher(Node node) {
        this.node = node;
        this.nodeEngine = node.getNodeEngine();
        this.logger = node.getLogger(WanBatchPublisher.class);
    }

    @Override
    public void init(WanReplicationConfig wanReplicationConfig, AbstractWanPublisherConfig publisherConfig) {
        this.wanReplicationName = wanReplicationConfig.getName();
        this.config = (WanBatchPublisherConfig) publisherConfig;
        this.publisherId = WanReplicationServiceImpl.getWanPublisherId(config);
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.queues = new WanPartitionQueue[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            queues[i] = new WanPartitionQueue();
        }
        this.state = config.getInitialPublisherState();
        this.endpointQualifier = config.getEndpoint() == null
                ? EndpointQualifier.MEMBER : EndpointQualifier.resolve(ProtocolType.WAN, config.getEndpoint());
        this.endpoints = new WanTargetEndpoints(config, logger, discoveryService());
        this.running = true;
        this.sender = new Thread(this::replicate, "hc." + nodeEngine.getHecatonInstance().getName()
                + ".wan." + wanReplicationName + "." + publisherId);
        sender.setDaemon(true);
        sender.start();
    }

    private Supplier<DiscoveryService> discoveryService() {
        List<DiscoveryStrategyConfig> aliased = AliasedDiscoveryConfigUtils.createDiscoveryStrategyConfigs(config);
        if (!config.getDiscoveryConfig().isEnabled() && aliased.isEmpty()) {
            return null;
        }
        DiscoveryService[] service = new DiscoveryService[1];
        return () -> {
            if (service[0] == null) {
                service[0] = node.createDiscoveryService(config.getDiscoveryConfig(), aliased, false,
                        node.getLocalMember());
                service[0].start();
            }
            return service[0];
        };
    }

    // publishing, on the partition threads

    @Override
    @SuppressWarnings("unchecked")
    public void publishReplicationEvent(WanEvent<Object> event) {
        if (!state.isEnqueueNewEvents()) {
            return;
        }
        InternalWanEvent<Object> wanEvent = (InternalWanEvent<Object>) event;
        if (outboundQueueSize() >= config.getQueueCapacity()) {
            countersOf(wanEvent).incrementDropped(wanEvent.getObjectName());
            return;
        }
        queueOf(wanEvent).add(wanEvent);
        wanEvent.incrementEventCount(countersOf(wanEvent));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void publishReplicationEventBackup(WanEvent<Object> event) {
        if (state.isEnqueueNewEvents()) {
            InternalWanEvent<Object> wanEvent = (InternalWanEvent<Object>) event;
            queueOf(wanEvent).add(wanEvent);
        }
    }

    @Override
    public void republishReplicationEvent(InternalWanEvent<Object> event) {
        publishReplicationEvent(event);
    }

    @Override
    public void doPrepublicationChecks() {
        if (outboundQueueSize() < config.getQueueCapacity()) {
            return;
        }
        switch (config.getQueueFullBehavior()) {
            case THROW_EXCEPTION -> throw queueFull();
            case THROW_EXCEPTION_ONLY_IF_REPLICATION_ACTIVE -> {
                if (state == WanPublisherState.REPLICATING) {
                    throw queueFull();
                }
            }
            default -> {
                // DISCARD_AFTER_MUTATION: the event is dropped when it is published
            }
        }
    }

    private WanQueueFullException queueFull() {
        return new WanQueueFullException("The WAN queue of publisher " + publisherId + " of WAN replication "
                + wanReplicationName + " is full (" + config.getQueueCapacity() + " events)");
    }

    private WanPartitionQueue queueOf(InternalWanEvent<?> event) {
        return queues[nodeEngine.getPartitionService().getPartitionId(event.getKey())];
    }

    private WanEventCounters countersOf(InternalWanEvent<?> event) {
        return nodeEngine.getWanReplicationService().getSentEventCounters(wanReplicationName, publisherId,
                event.getServiceName());
    }

    // replication, on the publisher's thread

    private void replicate() {
        long lastSend = System.currentTimeMillis();
        long park = config.getIdleMinParkNs();
        while (running) {
            try {
                if (!state.isReplicateEnqueuedEvents() || !nodeEngine.isRunning()) {
                    LockSupport.parkNanos(config.getIdleMaxParkNs());
                    continue;
                }
                Batch batch = collectBatch();
                if (batch.isEmpty() || (batch.size() < config.getBatchSize()
                        && System.currentTimeMillis() - lastSend < config.getBatchMaxDelayMillis())) {
                    LockSupport.parkNanos(park);
                    park = Math.min(park * 2, config.getIdleMaxParkNs());
                    continue;
                }
                park = config.getIdleMinParkNs();
                if (send(batch)) {
                    lastSend = System.currentTimeMillis();
                    removeSent(batch);
                } else {
                    Thread.sleep(FAILURE_BACKOFF_MILLIS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                if (running) {
                    logger.warning("WAN publisher " + publisherId + " of " + wanReplicationName + " failed", t);
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(FAILURE_BACKOFF_MILLIS));
                }
            }
        }
    }

    private Batch collectBatch() {
        Batch batch = new Batch();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionCount = queues.length;
        for (int i = 0; i < partitionCount && batch.size() < config.getBatchSize(); i++) {
            int partitionId = (nextPartition + i) % partitionCount;
            if (partitionService.getPartition(partitionId, false).isLocal() && !queues[partitionId].isEmpty()) {
                batch.add(partitionId, queues[partitionId].peek(config.getBatchSize() - batch.size()));
            }
        }
        nextPartition = (nextPartition + 1) % partitionCount;
        return batch;
    }

    /** @return whether the target acknowledged the batch */
    private boolean send(Batch batch) {
        Address target = endpoints.current();
        if (target == null) {
            connected = false;
            return false;
        }
        long start = System.nanoTime();
        try {
            ServerConnectionManager connectionManager = node.getServer().getConnectionManager(endpointQualifier);
            OperationService operationService = nodeEngine.getOperationService();
            operationService.createInvocationBuilder(WanReplicationService.SERVICE_NAME,
                            new WanBatchOperation(config.getClusterName(), batch.events(), config.getAcknowledgeType()),
                            target)
                    .setConnectionManager(connectionManager)
                    .setTryCount(1)
                    .setCallTimeout(config.getResponseTimeoutMillis())
                    .invoke()
                    .get(config.getResponseTimeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            if (connected) {
                logger.warning("Could not send WAN events to " + target + " of cluster " + config.getClusterName()
                        + ": " + e.getMessage());
            }
            connected = false;
            endpoints.failed(target);
            return false;
        }
        connected = true;
        long latency = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        for (int i = 0; i < batch.size(); i++) {
            stats.incrementPublishedEventCount(latency);
        }
        return true;
    }

    private void removeSent(Batch batch) {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        for (int i = 0; i < batch.partitions.size(); i++) {
            int partitionId = batch.partitions.get(i);
            Map<ServiceNamespace, Integer> removed = queues[partitionId].removeSent(batch.events.get(i), this::syncEventSent);
            if (!removed.isEmpty()) {
                removeFromBackups(partitionService.getPartition(partitionId, false), removed);
            }
        }
    }

    private void removeFromBackups(InternalPartition partition, Map<ServiceNamespace, Integer> removed) {
        OperationService operationService = nodeEngine.getOperationService();
        for (int replicaIndex = 1; replicaIndex < InternalPartition.MAX_REPLICA_COUNT; replicaIndex++) {
            PartitionReplica replica = partition.getReplica(replicaIndex);
            if (replica != null && !replica.uuid().equals(nodeEngine.getLocalMember().getUuid())) {
                operationService.send(new WanQueueRemoveOperation(
                        wanReplicationName, publisherId, partition.getPartitionId(), removed), replica.address());
            }
        }
    }

    /** Called on a backup: the owner sent these events. */
    void removeBackupEvents(int partitionId, Map<ServiceNamespace, Integer> counts) {
        queues[partitionId].removeFirst(counts);
    }

    /** The events of the partitions the member owns. */
    private int outboundQueueSize() {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int size = 0;
        for (int partitionId = 0; partitionId < queues.length; partitionId++) {
            int queued = queues[partitionId].size();
            if (queued > 0 && partitionService.getPartition(partitionId, false).isLocal()) {
                size += queued;
            }
        }
        return size;
    }

    /** The events in all queues, of owned and backup partitions. */
    int totalQueueSize() {
        int size = 0;
        for (WanPartitionQueue queue : queues) {
            size += queue.size();
        }
        return size;
    }

    // management

    @Override
    public void pause() {
        state = WanPublisherState.PAUSED;
    }

    @Override
    public void stop() {
        state = WanPublisherState.STOPPED;
    }

    @Override
    public void resume() {
        state = WanPublisherState.REPLICATING;
    }

    public WanPublisherState getState() {
        return state;
    }

    @Override
    public int removeWanEvents() {
        int removed = 0;
        for (WanPartitionQueue queue : queues) {
            removed += queue.clear();
        }
        return removed;
    }

    @Override
    public int removeWanEvents(int partitionId, String serviceName, String objectName, int count) {
        int removed = queues[partitionId].removeFirst(Map.of(
                new DistributedObjectNamespace(serviceName, objectName), count));
        return removed;
    }

    @Override
    public void destroyMapData(String mapName) {
        ServiceNamespace namespace = MapService.getObjectNamespace(mapName);
        for (WanPartitionQueue queue : queues) {
            queue.removeAll(namespace);
        }
    }

    @Override
    public LocalWanPublisherStats getStats() {
        stats.setConnected(connected);
        stats.setOutboundQueueSize(outboundQueueSize());
        stats.setState(state);
        stats.setSentMapEventCounter(sentCounters(MapService.SERVICE_NAME));
        stats.setSentCacheEventCounter(sentCounters(ICacheService.SERVICE_NAME));
        stats.setLastSyncStats(new HashMap<>(lastSyncStats));
        stats.setLastConsistencyCheckResults(new HashMap<>(lastConsistencyCheckResults));
        return stats;
    }

    private Map<String, WanEventCounters.DistributedObjectWanEventCounters> sentCounters(String serviceName) {
        return nodeEngine.getWanReplicationService().getSentEventCounters(wanReplicationName, publisherId, serviceName)
                .getEventCounterMap();
    }

    @Override
    public void reset() {
        removeWanEvents();
    }

    @Override
    public void shutdown() {
        running = false;
        if (sender != null) {
            sender.interrupt();
        }
    }

    // migration and replication of the queues

    @Override
    public void onMigrationStart(PartitionMigrationEvent event) {
        // the queue moves with the partition's data
    }

    @Override
    public void onMigrationCommit(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE && event.getNewReplicaIndex() == -1) {
            queues[event.getPartitionId()].clear();
        }
    }

    @Override
    public void onMigrationRollback(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION && event.getCurrentReplicaIndex() == -1) {
            queues[event.getPartitionId()].clear();
        }
    }

    @Override
    public Object prepareEventContainerReplicationData(PartitionReplicationEvent event,
                                                       Collection<ServiceNamespace> namespaces) {
        List<ServiceNamespace> replicated = new ArrayList<>(namespaces);
        return new WanPartitionEvents(replicated, queues[event.getPartitionId()].events(replicated));
    }

    @Override
    public void processEventContainerReplicationData(int partitionId, Object eventContainer) {
        WanPartitionEvents events = (WanPartitionEvents) eventContainer;
        queues[partitionId].replace(events.namespaces(), events.events());
    }

    @Override
    public void collectAllServiceNamespaces(PartitionReplicationEvent event, Set<ServiceNamespace> namespaces) {
        queues[event.getPartitionId()].collectNamespaces(namespaces);
    }

    /** The events of a batch, by partition. */
    private static final class Batch {
        private final List<Integer> partitions = new ArrayList<>();
        private final List<List<InternalWanEvent<Object>>> events = new ArrayList<>();
        private int size;

        void add(int partitionId, List<InternalWanEvent<Object>> partitionEvents) {
            partitions.add(partitionId);
            events.add(partitionEvents);
            size += partitionEvents.size();
        }

        int size() {
            return size;
        }

        boolean isEmpty() {
            return size == 0;
        }

        List<InternalWanEvent<Object>> events() {
            List<InternalWanEvent<Object>> all = new ArrayList<>(size);
            events.forEach(all::addAll);
            return all;
        }
    }

    // WAN sync and consistency check (WanSyncTask)

    /**
     * Starts a sync ({@code mode} {@code null}: full or Merkle tree sync, as the publisher's sync
     * config says) or a consistency check of the maps, for the partitions this member owns.
     */
    void startSync(UUID syncId, List<String> mapNames, WanSyncTask.Mode mode) {
        if (mode == null) {
            mode = config.getSyncConfig().getConsistencyCheckStrategy() == ConsistencyCheckStrategy.MERKLE_TREES
                    ? WanSyncTask.Mode.MERKLE : WanSyncTask.Mode.FULL;
        }
        WanSyncTask task = new WanSyncTask(this, nodeEngine, syncId, mapNames, mode);
        if (mode != WanSyncTask.Mode.CHECK) {
            syncs.put(syncId, task);
        }
        nodeEngine.getExecutionService().execute(SYNC_EXECUTOR, () -> {
            try {
                task.run();
            } finally {
                syncs.remove(syncId);
            }
        });
    }

    @SuppressWarnings("unchecked")
    void addSyncEvent(int partitionId, InternalWanEvent<?> event, UUID syncId) {
        queues[partitionId].addSync((InternalWanEvent<Object>) event, syncId);
    }

    private void syncEventSent(UUID syncId) {
        WanSyncTask task = syncs.get(syncId);
        if (task != null) {
            task.eventSent();
        }
    }

    boolean hasSyncEvents(UUID syncId) {
        for (WanPartitionQueue queue : queues) {
            if (queue.hasSyncEvents(syncId)) {
                return true;
            }
        }
        return false;
    }

    /** Whether a sync is running on this member. */
    public boolean isSyncing() {
        return !syncs.isEmpty();
    }

    void synced(String mapName, WanSyncStats syncStats) {
        lastSyncStats.put(mapName, syncStats);
    }

    void syncFailed() {
        syncFailed = true;
    }

    /** Whether the last sync failed on this member. */
    public boolean hasSyncFailed() {
        return syncFailed;
    }

    void consistencyChecked(String mapName, ConsistencyCheckResult result) {
        lastConsistencyCheckResults.put(mapName, result);
    }

    /** Asks the target cluster for the Merkle tree leaves of a map in the given partitions; tries each endpoint. */
    @SuppressWarnings("unchecked")
    Map<Integer, int[]> queryTargetLeaves(String mapName, int[] partitionIds) throws Exception {
        Exception failure = null;
        for (int attempt = 0; attempt < Math.max(1, endpoints.count()); attempt++) {
            Address target = endpoints.current();
            if (target == null) {
                break;
            }
            try {
                return (Map<Integer, int[]>) nodeEngine.getOperationService().createInvocationBuilder(
                                WanReplicationService.SERVICE_NAME,
                                new WanMerkleTreeQueryOperation(config.getClusterName(), mapName, partitionIds), target)
                        .setConnectionManager(node.getServer().getConnectionManager(endpointQualifier))
                        .setTryCount(1)
                        .setCallTimeout(config.getResponseTimeoutMillis())
                        .invoke()
                        .get(config.getResponseTimeoutMillis(), TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                failure = e;
                endpoints.failed(target);
            }
        }
        throw failure != null ? failure : new IllegalStateException("No endpoint of WAN target cluster "
                + config.getClusterName());
    }

    String publisherId() {
        return publisherId;
    }

    public String getPublisherId() {
        return publisherId;
    }

    String wanReplicationName() {
        return wanReplicationName;
    }

    ManagementCenterService managementCenter() {
        return node.getManagementCenterService();
    }

    boolean isRunning() {
        return running;
    }

}
