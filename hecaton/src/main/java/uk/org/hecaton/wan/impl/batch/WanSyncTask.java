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

import uk.org.hecaton.internal.management.ManagementCenterService;
import uk.org.hecaton.internal.management.events.Event;
import uk.org.hecaton.internal.management.events.WanConsistencyCheckFinishedEvent;
import uk.org.hecaton.internal.management.events.WanConsistencyCheckStartedEvent;
import uk.org.hecaton.internal.management.events.WanFullSyncFinishedEvent;
import uk.org.hecaton.internal.management.events.WanMerkleSyncFinishedEvent;
import uk.org.hecaton.internal.management.events.WanSyncProgressUpdateEvent;
import uk.org.hecaton.internal.management.events.WanSyncStartedEvent;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.wan.impl.ConsistencyCheckResult;
import uk.org.hecaton.wan.impl.merkletree.MerkleTreeUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A WAN sync or consistency check of some maps, run by the publisher of each member for the
 * partitions it owns (F13).
 * <ul>
 *     <li>{@link Mode#FULL}: every entry is sent to the target cluster as a sync event;</li>
 *     <li>{@link Mode#MERKLE}: the Merkle tree leaves of each partition are compared with the target
 *     cluster's, and the entries of the leaves that differ are sent;</li>
 *     <li>{@link Mode#CHECK}: the comparison only, reported in the publisher's statistics.</li>
 * </ul>
 * Sync events go through the partitions' queues, in order with the other events. The sync is finished
 * once the target acknowledged all of them. Entries that exist only in the target cluster are not
 * removed.
 */
class WanSyncTask implements Runnable {

    enum Mode { FULL, MERKLE, CHECK }

    private static final long COMPLETION_POLL_MILLIS = 100;

    private final WanBatchPublisher publisher;
    private final NodeEngine nodeEngine;
    private final ILogger logger;
    private final UUID syncId;
    private final List<String> mapNames;
    private final Mode mode;
    private final AtomicLong queued = new AtomicLong();
    private final AtomicLong sent = new AtomicLong();

    WanSyncTask(WanBatchPublisher publisher, NodeEngine nodeEngine, UUID syncId, List<String> mapNames, Mode mode) {
        this.publisher = publisher;
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(WanSyncTask.class);
        this.syncId = syncId;
        this.mapNames = mapNames;
        this.mode = mode;
    }

    UUID id() {
        return syncId;
    }

    /** One sync event of this sync was acknowledged by the target. */
    void eventSent() {
        sent.incrementAndGet();
    }

    @Override
    public void run() {
        for (String mapName : mapNames) {
            try {
                runFor(mapName);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                logger.warning("WAN " + mode + " of map " + mapName + " to " + publisher.publisherId() + " failed", e);
                publisher.syncFailed();
            }
        }
    }

    private void runFor(String mapName) throws Exception {
        int[] partitions = ownedPartitions();
        if (mode == Mode.CHECK) {
            log(new WanConsistencyCheckStartedEvent(syncId, publisher.wanReplicationName(), publisher.publisherId(),
                    mapName));
            check(mapName, partitions);
        } else {
            log(new WanSyncStartedEvent(syncId, publisher.wanReplicationName(), publisher.publisherId(), mapName));
            sync(mapName, partitions);
        }
    }

    private void check(String mapName, int[] partitions) throws Exception {
        Map<Integer, Set<Integer>> leavesByPartition = differingLeaves(mapName, partitions);
        int depth = depth(mapName);
        int diffLeaves = 0;
        int diffPartitions = 0;
        long entries = 0;
        for (int partitionId : partitions) {
            Set<Integer> leaves = leavesByPartition.get(partitionId);
            if (!leaves.isEmpty()) {
                diffPartitions++;
                diffLeaves += leaves.size();
                entries += runOnPartition(mapName, partitionId, leaves, depth);
            }
        }
        int checkedLeaves = partitions.length * MerkleTreeUtil.getNodesOnLevel(depth - 1);
        publisher.consistencyChecked(mapName, new ConsistencyCheckResult(syncId, partitions.length, diffPartitions,
                checkedLeaves, diffLeaves, (int) entries));
        log(new WanConsistencyCheckFinishedEvent(syncId, publisher.wanReplicationName(), publisher.publisherId(),
                mapName, diffPartitions, partitions.length, (int) entries));
    }

    private void sync(String mapName, int[] partitions) throws Exception {
        long start = System.nanoTime();
        long sentBefore = sent.get();
        Map<Integer, Set<Integer>> leavesByPartition = mode == Mode.FULL ? null : differingLeaves(mapName, partitions);
        int depth = mode == Mode.FULL ? 0 : depth(mapName);
        int partitionsSynced = 0;
        int diffLeaves = 0;
        for (int partitionId : partitions) {
            Set<Integer> leaves = leavesByPartition == null ? null : leavesByPartition.get(partitionId);
            if (leaves != null && leaves.isEmpty()) {
                continue;
            }
            diffLeaves += leaves == null ? 0 : leaves.size();
            queued.addAndGet(runOnPartition(mapName, partitionId, leaves, depth));
            partitionsSynced++;
            log(new WanSyncProgressUpdateEvent(syncId, publisher.wanReplicationName(), publisher.publisherId(),
                    mapName, partitions.length, partitionsSynced, (int) (sent.get() - sentBefore)));
        }
        awaitSent();
        long durationSecs = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);
        // events dropped meanwhile (the queue was cleared, the partition migrated) are not counted
        long synced = sent.get() - sentBefore;
        publisher.synced(mapName, new WanSyncStatsImpl(syncId, durationSecs, partitions.length, partitionsSynced,
                (int) synced));
        if (mode == Mode.FULL) {
            log(new WanFullSyncFinishedEvent(syncId, publisher.wanReplicationName(), publisher.publisherId(), mapName,
                    durationSecs, synced, partitionsSynced));
        } else {
            double perLeaf = diffLeaves == 0 ? 0 : (double) synced / diffLeaves;
            log(new WanMerkleSyncFinishedEvent(syncId, publisher.wanReplicationName(), publisher.publisherId(),
                    mapName, durationSecs, partitionsSynced, diffLeaves, synced, 0, 0, perLeaf, 0));
        }
    }

    /** Queues (or, for a check, counts) the entries of a partition, of the given leaves if not null. */
    private int runOnPartition(String mapName, int partitionId, Set<Integer> leaves, int depth) {
        return nodeEngine.getOperationService().<Integer>invokeOnPartition(
                new WanSyncPartitionOperation(publisher, mapName, syncId, leaves, depth, mode == Mode.CHECK)
                        .setPartitionId(partitionId)).join();
    }

    /** Per partition, the Merkle tree leaves where this cluster and the target differ. */
    private Map<Integer, Set<Integer>> differingLeaves(String mapName, int[] partitions) throws Exception {
        Map<Integer, int[]> target = publisher.queryTargetLeaves(mapName, partitions);
        Map<Integer, Set<Integer>> differing = new HashMap<>();
        int depth = depth(mapName);
        int firstLeaf = MerkleTreeUtil.getLeftMostNodeOrderOnLevel(depth - 1);
        for (int partitionId : partitions) {
            int[] local = nodeEngine.getOperationService().<int[]>invokeOnPartition(
                    new MerkleTreeLeavesOperation(mapName).setPartitionId(partitionId)).join();
            if (local == null) {
                throw new IllegalStateException("Map " + mapName + " keeps no Merkle tree: enable merkle-tree on it");
            }
            int[] remote = target.get(partitionId);
            Set<Integer> leaves = new HashSet<>();
            for (int i = 0; i < local.length; i++) {
                // a target without a tree of the same depth differs everywhere
                if (remote == null || remote.length != local.length || remote[i] != local[i]) {
                    leaves.add(firstLeaf + i);
                }
            }
            differing.put(partitionId, leaves);
        }
        return differing;
    }

    private int depth(String mapName) {
        return nodeEngine.getConfig().findMapConfig(mapName).getMerkleTreeConfig().getDepth();
    }

    private int[] ownedPartitions() {
        IPartitionService partitionService = nodeEngine.getPartitionService();
        List<Integer> owned = new ArrayList<>();
        for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
            if (partitionService.isPartitionOwner(partitionId)) {
                owned.add(partitionId);
            }
        }
        return owned.stream().mapToInt(Integer::intValue).toArray();
    }

    private void awaitSent() throws InterruptedException {
        while (sent.get() < queued.get() && publisher.isRunning() && publisher.hasSyncEvents(syncId)) {
            Thread.sleep(COMPLETION_POLL_MILLIS);
        }
    }

    private void log(Event event) {
        ManagementCenterService mc = publisher.managementCenter();
        if (mc != null) {
            mc.log(event);
        }
    }
}
