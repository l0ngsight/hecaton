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
package uk.org.hecaton.persistence.internal.cluster;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.PersistenceConfig;
import uk.org.hecaton.config.HotRestartClusterDataRecoveryPolicy;
import uk.org.hecaton.hotrestart.HotRestartService;
import uk.org.hecaton.instance.impl.ClusterTopologyIntent;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.cluster.impl.operations.OnJoinOp;
import uk.org.hecaton.internal.hotrestart.InternalHotRestartService;
import uk.org.hecaton.internal.management.dto.ClusterHotRestartStatusDTO;
import uk.org.hecaton.internal.management.dto.ClusterHotRestartStatusDTO.ClusterHotRestartStatus;
import uk.org.hecaton.internal.management.dto.ClusterHotRestartStatusDTO.MemberHotRestartStatus;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.internal.partition.PartitionRuntimeState;
import uk.org.hecaton.internal.partition.PartitionTableView;
import uk.org.hecaton.internal.partition.impl.InternalPartitionServiceImpl;
import uk.org.hecaton.internal.partition.operation.SafeStateCheckOperation;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.persistence.BackupTaskState;
import uk.org.hecaton.persistence.BackupTaskStatus;
import uk.org.hecaton.spi.impl.PartitionSpecificRunnable;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.version.Version;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The member's side of persistence: keeps the cluster metadata on disk and, when the member
 * starts with persisted data, takes part in the recovery of the cluster.
 * <p>
 * Recovery is driven by the master, which, in turn:
 * <ol>
 * <li>waits for the members that held partition replicas before the restart (up to the
 * validation timeout);</li>
 * <li>checks that they persisted the same partition table;</li>
 * <li>asks every member to load its data for that table, and waits for them (up to the data
 * load timeout);</li>
 * <li>asks every member to install the table and complete its start.</li>
 * </ol>
 * Until the table is installed, no partition has an owner, so partition operations wait. If the
 * master leaves, the next one starts over; every step can be repeated.
 */
@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:MethodCount"})
public final class RecoveryService implements InternalHotRestartService, HotRestartService {

    private static final long POLL_MILLIS = 100;

    private final Node node;
    private final ILogger logger;
    private final ClusterMetadata metadata;
    private final LocalData data;
    private final PersistenceConfig config;
    private final UUID localUuid;
    /** The table persisted before the restart; null if the member had no data. */
    private final PartitionTableSnapshot persistedTable;
    private final ClusterState persistedState;
    private final List<OnJoinOp> deferredPostJoinOps = new ArrayList<>();
    private final Object writeLock = new Object();

    private volatile boolean startCompleted;
    private volatile int loadState = RecoveryStatus.NOT_LOADED;
    private volatile String loadFailure;
    private volatile ClusterHotRestartStatus clusterStatus = ClusterHotRestartStatus.UNKNOWN;
    private volatile long validationDeadline = -1;
    private volatile long loadDeadline = -1;
    private long writtenTableVersionSum = -1;

    public RecoveryService(Node node, ClusterMetadata metadata, LocalData data, UUID localUuid) {
        this.node = node;
        this.logger = node.getLogger(RecoveryService.class);
        this.metadata = metadata;
        this.data = data;
        this.config = node.getConfig().getPersistenceConfig();
        this.localUuid = localUuid;
        PartitionTableSnapshot table = metadata.readPartitions();
        this.persistedTable = table != null && table.hasReplicas() ? table : null;
        this.persistedState = metadata.readState();
        this.startCompleted = persistedTable == null;
        if (persistedTable == null) {
            clusterStatus = ClusterHotRestartStatus.SUCCEEDED;
        }
    }

    // lifecycle, called by the module

    /** Called once the member has joined. */
    public void afterStart() {
        if (startCompleted) {
            writeMetadata();
            return;
        }
        clusterStatus = ClusterHotRestartStatus.IN_PROGRESS;
        logger.info("Recovering from persistence: this member held replicas of "
                + countReplicas() + " partitions before the restart");
        Thread thread = new Thread(this::recover, "hc.persistence.recovery");
        thread.setDaemon(true);
        thread.start();
    }

    public void onMemberListChange() {
        if (startCompleted) {
            ClusterService clusterService = node.getClusterService();
            synchronized (writeLock) {
                metadata.writeMembers(clusterService.getMemberListVersion(), clusterService.getMembers());
            }
        }
    }

    public void onPartitionStateChange() {
        if (startCompleted) {
            writePartitionTable(node.getPartitionService().createPartitionTableView());
        }
    }

    public void onClusterStateChange(ClusterState newState) {
        if (startCompleted && newState != ClusterState.IN_TRANSITION) {
            synchronized (writeLock) {
                metadata.writeState(newState);
            }
        }
    }

    public void onClusterVersionChange(Version newVersion) {
        // the cluster version is the codebase version of the members; nothing to keep
    }

    private void writeMetadata() {
        onMemberListChange();
        onPartitionStateChange();
        onClusterStateChange(node.getClusterService().getClusterState());
    }

    private void writePartitionTable(PartitionTableView view) {
        PartitionTableSnapshot snapshot = PartitionTableSnapshot.from(view);
        if (!snapshot.hasReplicas()) {
            return;
        }
        synchronized (writeLock) {
            // the views of concurrent changes may arrive out of order; versions only grow
            if (snapshot.versionSum() >= writtenTableVersionSum) {
                metadata.writePartitions(view);
                writtenTableVersionSum = snapshot.versionSum();
            }
        }
    }

    // recovery

    private void recover() {
        validationDeadline = System.currentTimeMillis() + SECONDS.toMillis(config.getValidationTimeoutSeconds());
        while (!startCompleted && node.isRunning() && clusterStatus != ClusterHotRestartStatus.FAILED) {
            try {
                if (node.isMaster()) {
                    coordinate();
                } else {
                    sleep();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                if (node.isRunning()) {
                    logger.warning("Recovery from persistence: " + e.getMessage() + "; retrying", e);
                }
                sleepQuietly();
            }
        }
    }

    /** Runs the recovery as the master; returns early if the member stops being the master. */
    private void coordinate() throws Exception {
        Set<UUID> expected = persistedTable.members();
        if (!waitForMembers(expected) || !validate(expected)) {
            return;
        }
        logger.info("Recovery from persistence: all " + expected.size() + " members are present; loading data");
        invokeOnAll(RecoveryOperation.LOAD);
        loadDeadline = System.currentTimeMillis() + SECONDS.toMillis(config.getDataLoadTimeoutSeconds());
        while (!allLoaded(statuses())) {
            if (System.currentTimeMillis() > loadDeadline) {
                fail("the members did not load their data within the data load timeout ("
                        + config.getDataLoadTimeoutSeconds() + " seconds)");
                return;
            }
            if (!node.isMaster() || clusterStatus == ClusterHotRestartStatus.FAILED) {
                return;
            }
            sleep();
        }
        invokeOnAll(RecoveryOperation.COMPLETE);
        restoreClusterState();
    }

    /** @return whether the members are present; false if the time ran out or this member is no longer the master */
    private boolean waitForMembers(Set<UUID> expected) throws InterruptedException {
        while (!presentMembers().keySet().containsAll(expected)) {
            if (System.currentTimeMillis() > validationDeadline) {
                Set<UUID> missing = new HashSet<>(expected);
                missing.removeAll(presentMembers().keySet());
                fail("members " + missing + " did not join within the validation timeout ("
                        + config.getValidationTimeoutSeconds() + " seconds)");
                return false;
            }
            if (!node.isMaster()) {
                return false;
            }
            sleep();
        }
        return true;
    }

    /** @return whether the members persisted the same partition table as this one */
    private boolean validate(Set<UUID> expected) throws Exception {
        if (persistedTable.partitionCount() != node.getPartitionService().getPartitionCount()) {
            fail("the partition count changed from " + persistedTable.partitionCount() + " to "
                    + node.getPartitionService().getPartitionCount());
            return false;
        }
        Map<UUID, RecoveryStatus> statuses = statuses();
        for (UUID member : expected) {
            RecoveryStatus status = statuses.get(member);
            if (status == null || !status.hasData() || status.tableStamp() != persistedTable.stamp()) {
                fail("member " + member + " persisted a different partition table");
                return false;
            }
        }
        return true;
    }

    private boolean allLoaded(Map<UUID, RecoveryStatus> statuses) {
        boolean all = true;
        for (Map.Entry<UUID, RecoveryStatus> status : statuses.entrySet()) {
            if (status.getValue().loadState() == RecoveryStatus.LOAD_FAILED) {
                fail("member " + status.getKey() + " could not load its data: " + status.getValue().failure());
                return false;
            }
            all &= status.getValue().loadState() == RecoveryStatus.LOADED;
        }
        return all;
    }

    private void restoreClusterState() {
        ClusterService clusterService = node.getClusterService();
        // a cluster shut down as a whole was PASSIVE at the end; it comes back ACTIVE
        if (persistedState != null && persistedState != ClusterState.PASSIVE
                && persistedState != clusterService.getClusterState()) {
            try {
                clusterService.changeClusterState(persistedState);
            } catch (RuntimeException e) {
                logger.warning("Could not restore the cluster state " + persistedState, e);
            }
        }
    }

    private void fail(String reason) {
        clusterStatus = ClusterHotRestartStatus.FAILED;
        logger.severe("Recovery from persistence failed: " + reason + ". The cluster cannot start with its data; "
                + "restart the missing members, or force start the cluster to start it without its data.");
    }

    private Map<UUID, Address> presentMembers() {
        Map<UUID, Address> members = new HashMap<>();
        for (Member member : node.getClusterService().getMembers()) {
            members.put(member.getUuid(), member.getAddress());
        }
        return members;
    }

    private Map<UUID, RecoveryStatus> statuses() throws Exception {
        Map<UUID, RecoveryStatus> statuses = new HashMap<>();
        OperationService operationService = node.getNodeEngine().getOperationService();
        for (Member member : node.getClusterService().getMembers()) {
            RecoveryStatus status = member.localMember() ? status()
                    : operationService.<RecoveryStatus>invokeOnTarget(SERVICE_NAME,
                            new RecoveryOperation(RecoveryOperation.STATUS, null), member.getAddress()).get();
            statuses.put(member.getUuid(), status);
        }
        return statuses;
    }

    private void invokeOnAll(int step) throws Exception {
        OperationService operationService = node.getNodeEngine().getOperationService();
        Member local = null;
        for (Member member : node.getClusterService().getMembers()) {
            if (member.localMember()) {
                local = member;
            } else {
                operationService.invokeOnTarget(SERVICE_NAME, new RecoveryOperation(step, persistedTable),
                        member.getAddress()).get();
            }
        }
        if (local != null) {
            if (step == RecoveryOperation.LOAD) {
                startLoading(persistedTable);
            } else {
                complete(persistedTable);
            }
        }
    }

    // steps run on every member

    RecoveryStatus status() {
        return persistedTable == null
                ? new RecoveryStatus(false, 0, 0, startCompleted ? RecoveryStatus.LOADED : loadState, loadFailure)
                : new RecoveryStatus(true, persistedTable.stamp(), persistedTable.versionSum(), loadState, loadFailure);
    }

    synchronized void startLoading(PartitionTableSnapshot table) {
        if (startCompleted || loadState != RecoveryStatus.NOT_LOADED) {
            return;
        }
        loadState = RecoveryStatus.LOADING;
        Thread thread = new Thread(() -> load(table), "hc.persistence.load");
        thread.setDaemon(true);
        thread.start();
    }

    private void load(PartitionTableSnapshot table) {
        long start = System.currentTimeMillis();
        int count = table.partitionCount();
        CountDownLatch done = new CountDownLatch(count);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        OperationService operationService = node.getNodeEngine().getOperationService();
        for (int p = 0; p < count; p++) {
            int partitionId = p;
            int replicaIndex = table.replicaIndexOf(p, localUuid);
            operationService.execute(new PartitionSpecificRunnable() {
                @Override
                public int getPartitionId() {
                    return partitionId;
                }

                @Override
                public void run() {
                    try {
                        if (replicaIndex < 0) {
                            if (data.hasPartition(partitionId)) {
                                data.delete(partitionId);
                            }
                        } else if (data.hasPartition(partitionId)) {
                            data.load(partitionId, replicaIndex == 0);
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    } finally {
                        done.countDown();
                    }
                }
            });
        }
        try {
            done.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failure.compareAndSet(null, e);
        }
        if (failure.get() != null) {
            logger.severe("Could not load the persisted data", failure.get());
            loadFailure = String.valueOf(failure.get());
            loadState = RecoveryStatus.LOAD_FAILED;
        } else {
            logger.info("Loaded the persisted data in " + (System.currentTimeMillis() - start) + " ms");
            loadState = RecoveryStatus.LOADED;
        }
    }

    synchronized void complete(PartitionTableSnapshot table) {
        if (startCompleted) {
            return;
        }
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) node.getPartitionService();
        if (!partitionService.isPartitionAssignmentDone()) {
            partitionService.setInitialState(table.toView(presentMembers()));
        }
        startCompleted = true;
        clusterStatus = ClusterHotRestartStatus.SUCCEEDED;
        List<OnJoinOp> postJoinOps;
        synchronized (deferredPostJoinOps) {
            postJoinOps = new ArrayList<>(deferredPostJoinOps);
            deferredPostJoinOps.clear();
        }
        for (OnJoinOp op : postJoinOps) {
            node.getNodeEngine().getOperationService().run(op);
        }
        writeMetadata();
        logger.info("Recovery from persistence completed");
    }

    private int countReplicas() {
        int count = 0;
        for (int p = 0; p < persistedTable.partitionCount(); p++) {
            if (persistedTable.replicaIndexOf(p, localUuid) >= 0) {
                count++;
            }
        }
        return count;
    }

    private static void sleep() throws InterruptedException {
        Thread.sleep(POLL_MILLIS);
    }

    private static void sleepQuietly() {
        try {
            sleep();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // InternalHotRestartService

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public boolean trySetDeferredClusterState(ClusterState newClusterState) {
        return false;
    }

    @Override
    public boolean isStartCompleted() {
        return startCompleted;
    }

    @Override
    public boolean triggerForceStart() {
        return false;
    }

    @Override
    public boolean triggerPartialStart() {
        return false;
    }

    @Override
    public boolean isMemberExcluded(Address memberAddress, UUID memberUuid) {
        return false;
    }

    @Override
    public Set<UUID> getExcludedMemberUuids() {
        return Collections.emptySet();
    }

    @Override
    public void notifyExcludedMember(Address memberAddress) {
        // members are not excluded yet
    }

    @Override
    public void handleExcludedMemberUuids(Address sender, Set<UUID> excludedMemberUuids) {
        // members are not excluded yet
    }

    @Override
    public ClusterHotRestartStatusDTO getCurrentClusterHotRestartStatus() {
        long now = System.currentTimeMillis();
        Map<String, MemberHotRestartStatus> members = new HashMap<>();
        MemberHotRestartStatus local = switch (clusterStatus) {
            case SUCCEEDED -> MemberHotRestartStatus.SUCCESSFUL;
            case FAILED -> MemberHotRestartStatus.FAILED;
            default -> loadState == RecoveryStatus.LOADING ? MemberHotRestartStatus.LOAD_IN_PROGRESS
                    : MemberHotRestartStatus.PENDING;
        };
        members.put(node.getThisAddress().toString(), local);
        return new ClusterHotRestartStatusDTO(
                HotRestartClusterDataRecoveryPolicy.valueOf(config.getClusterDataRecoveryPolicy().name()),
                clusterStatus,
                validationDeadline < 0 ? -1 : Math.max(0, validationDeadline - now),
                loadDeadline < 0 ? -1 : Math.max(0, loadDeadline - now),
                members);
    }

    @Override
    public void resetService(boolean isAfterJoin) {
        // the record stores are reset by their services, which clears their persisted data
    }

    @Override
    public void forceStartBeforeJoin() {
        // members are not excluded yet
    }

    @Override
    public void waitPartitionReplicaSyncOnCluster(long timeout, TimeUnit unit,
                                                  Supplier<SafeStateCheckOperation> supplier) {
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        OperationService operationService = node.getNodeEngine().getOperationService();
        while (System.currentTimeMillis() < deadline) {
            boolean safe = true;
            for (Member member : node.getClusterService().getMembers()) {
                try {
                    Boolean memberSafe = operationService.<Boolean>invokeOnTarget(InternalPartitionService.SERVICE_NAME,
                            supplier.get(), member.getAddress()).get();
                    safe &= Boolean.TRUE.equals(memberSafe);
                } catch (Exception e) {
                    safe = false;
                }
            }
            if (safe) {
                return;
            }
            sleepQuietly();
        }
        logger.warning("Partition replicas were not in sync within " + unit.toMillis(timeout) + " ms");
    }

    @Override
    public void setRejoiningActiveCluster(boolean rejoiningActiveCluster) {
        // a member rejoining a running cluster recovers like any other
    }

    @Override
    public void deferApplyPartitionState(@Nullable PartitionRuntimeState partitionRuntimeState) {
        // the master publishes the partition table again once recovery completes
    }

    @Override
    public void deferPostJoinOps(OnJoinOp postJoinOp) {
        synchronized (deferredPostJoinOps) {
            if (startCompleted) {
                node.getNodeEngine().getOperationService().run(postJoinOp);
            } else {
                deferredPostJoinOps.add(postJoinOp);
            }
        }
    }

    @Override
    public boolean isClusterMetadataFoundOnDisk() {
        return persistedTable != null;
    }

    @Override
    public void onClusterTopologyIntentChange() {
        // no managed-context (Kubernetes) restarts yet
    }

    @Override
    public void setClusterTopologyIntentOnMaster(ClusterTopologyIntent clusterTopologyIntent) {
        // no managed-context (Kubernetes) restarts yet
    }

    @Override
    public void onMemberLeft(Member member, ClusterState clusterState) {
        // a member leaving during recovery is waited for by the master
    }

    @Override
    public void onMemberJoined(Member member) {
        // members joining during recovery are seen by the master
    }

    // HotRestartService (backups come with F10.5)

    @Override
    public void backup() {
        throw new UnsupportedOperationException("Persistence backups are not available yet");
    }

    @Override
    public void backup(long backupSeq) {
        backup();
    }

    @Override
    public BackupTaskStatus getBackupTaskStatus() {
        return new BackupTaskStatus(BackupTaskState.NO_TASK, 0, 0);
    }

    @Override
    public void interruptLocalBackupTask() {
        // no backup runs
    }

    @Override
    public void interruptBackupTask() {
        // no backup runs
    }

    @Override
    public boolean isHotBackupEnabled() {
        return false;
    }

    @Override
    public boolean isBackupEnabled() {
        return false;
    }

    @Override
    public String getBackupDirectory() {
        return config.getBackupDir() == null ? null : config.getBackupDir().getAbsolutePath();
    }
}
