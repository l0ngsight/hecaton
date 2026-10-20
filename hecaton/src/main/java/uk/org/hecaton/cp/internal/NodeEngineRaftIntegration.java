/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.cp.internal;

import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.CPMember;
import uk.org.hecaton.cp.internal.operation.integration.AppendFailureResponseOp;
import uk.org.hecaton.cp.internal.operation.integration.AppendRequestOp;
import uk.org.hecaton.cp.internal.operation.integration.AppendSuccessResponseOp;
import uk.org.hecaton.cp.internal.operation.integration.AsyncRaftOp;
import uk.org.hecaton.cp.internal.operation.integration.InstallSnapshotOp;
import uk.org.hecaton.cp.internal.operation.integration.PreVoteRequestOp;
import uk.org.hecaton.cp.internal.operation.integration.PreVoteResponseOp;
import uk.org.hecaton.cp.internal.operation.integration.TriggerLeaderElectionOp;
import uk.org.hecaton.cp.internal.operation.integration.VoteRequestOp;
import uk.org.hecaton.cp.internal.operation.integration.VoteResponseOp;
import uk.org.hecaton.cp.internal.raft.SnapshotAwareService;
import uk.org.hecaton.cp.internal.raft.impl.RaftEndpoint;
import uk.org.hecaton.cp.internal.raft.impl.RaftIntegration;
import uk.org.hecaton.cp.internal.raft.impl.RaftNodeStatus;
import uk.org.hecaton.cp.internal.raft.impl.dto.AppendFailureResponse;
import uk.org.hecaton.cp.internal.raft.impl.dto.AppendRequest;
import uk.org.hecaton.cp.internal.raft.impl.dto.AppendSuccessResponse;
import uk.org.hecaton.cp.internal.raft.impl.dto.InstallSnapshot;
import uk.org.hecaton.cp.internal.raft.impl.dto.PreVoteRequest;
import uk.org.hecaton.cp.internal.raft.impl.dto.PreVoteResponse;
import uk.org.hecaton.cp.internal.raft.impl.dto.TriggerLeaderElection;
import uk.org.hecaton.cp.internal.raft.impl.dto.VoteRequest;
import uk.org.hecaton.cp.internal.raft.impl.dto.VoteResponse;
import uk.org.hecaton.cp.internal.raftop.NotifyTermChangeOp;
import uk.org.hecaton.cp.internal.raftop.snapshot.RestoreSnapshotOp;
import uk.org.hecaton.cp.internal.util.PartitionSpecificRunnableAdaptor;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.executionservice.TaskScheduler;
import uk.org.hecaton.spi.impl.operationexecutor.OperationExecutor;
import uk.org.hecaton.spi.impl.operationexecutor.impl.PartitionOperationThread;
import uk.org.hecaton.spi.impl.operationservice.impl.OperationServiceImpl;
import uk.org.hecaton.spi.impl.servicemanager.ServiceInfo;
import uk.org.hecaton.spi.properties.HecatonProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.cp.internal.RaftService.CP_SUBSYSTEM_EXECUTOR;
import static uk.org.hecaton.cp.internal.raft.impl.RaftNodeStatus.STEPPED_DOWN;
import static uk.org.hecaton.cp.internal.raft.impl.RaftNodeStatus.TERMINATED;

/**
 * The integration point of the Raft algorithm implementation and
 * Hecaton system. Replicates Raft RPCs via Hecaton operations and executes
 * committed Raft operations.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
final class NodeEngineRaftIntegration implements RaftIntegration {

    /**
     * !!! ONLY FOR INTERNAL USAGE AND TESTING !!!
     * Enables / disables the linearizable read optimization described in the Raft Dissertation Section 6.4.
     */
    public static final HecatonProperty RAFT_LINEARIZABLE_READ_OPTIMIZATION_ENABLED
            = new HecatonProperty("raft.linearizable.read.optimization.enabled", true);


    private final NodeEngineImpl nodeEngine;
    private final CPGroupId groupId;
    private final RaftEndpoint localCPMember;
    private final Address localAddress;
    private final OperationServiceImpl operationService;
    private final RaftInvocationManager invocationManager;
    private final TaskScheduler taskScheduler;
    private final int partitionId;
    private final int threadId;
    private final boolean linearizableReadOptimizationEnabled;

    NodeEngineRaftIntegration(NodeEngineImpl nodeEngine, CPGroupId groupId, RaftEndpoint localCPMember, int partitionId) {
        this.nodeEngine = nodeEngine;
        this.groupId = groupId;
        this.localCPMember = localCPMember;
        this.localAddress = nodeEngine.getThisAddress();
        this.operationService = nodeEngine.getOperationService();
        this.invocationManager = ((RaftService) nodeEngine.getService(RaftService.SERVICE_NAME)).getInvocationManager();
        this.partitionId = partitionId;
        OperationExecutor operationExecutor = operationService.getOperationExecutor();
        this.threadId = operationExecutor.toPartitionThreadIndex(partitionId);
        this.taskScheduler = nodeEngine.getExecutionService().getGlobalTaskScheduler();
        this.linearizableReadOptimizationEnabled = nodeEngine.getProperties()
                                                             .getBoolean(RAFT_LINEARIZABLE_READ_OPTIMIZATION_ENABLED);
    }

    @Override
    public void execute(Runnable task) {
        Thread currentThread = Thread.currentThread();
        if (currentThread instanceof PartitionOperationThread
                && ((PartitionOperationThread) currentThread).getThreadId() == threadId) {
            task.run();
        } else {
            operationService.execute(new PartitionSpecificRunnableAdaptor(task, partitionId));
        }
    }

    @Override
    public void submit(Runnable task) {
        operationService.execute(new PartitionSpecificRunnableAdaptor(task, partitionId));
    }

    @Override
    public void schedule(final Runnable task, long delay, TimeUnit timeUnit) {
        taskScheduler.schedule(() -> execute(task), delay, timeUnit);
    }

    @Override
    public InternalCompletableFuture newCompletableFuture() {
        return new InternalCompletableFuture();
    }

    @Override
    public Object getAppendedEntryOnLeaderElection() {
        return new NotifyTermChangeOp();
    }

    @Override
    public boolean isLinearizableReadOptimizationEnabled() {
        return linearizableReadOptimizationEnabled;
    }

    @Override
    public ILogger getLogger(String name) {
        return nodeEngine.getLogger(name);
    }

    @Override
    public boolean isReady() {
        return nodeEngine.getClusterService().isJoined();
    }

    @Override
    public boolean isReachable(RaftEndpoint target) {
        if (!isStartCompleted()) {
            return true;
        }

        CPMember targetMember = getCPMember(target);
        return targetMember != null && nodeEngine.getClusterService().getMember(targetMember.getAddress()) != null;
    }

    private boolean isStartCompleted() {
        return nodeEngine.getNode().getNodeExtension().isStartCompleted();
    }

    @Override
    public boolean send(PreVoteRequest request, RaftEndpoint target) {
        return send(new PreVoteRequestOp(groupId, request), target);
    }

    @Override
    public boolean send(PreVoteResponse response, RaftEndpoint target) {
        return send(new PreVoteResponseOp(groupId, response), target);
    }

    @Override
    public boolean send(VoteRequest request, RaftEndpoint target) {
        return send(new VoteRequestOp(groupId, request), target);
    }

    @Override
    public boolean send(VoteResponse response, RaftEndpoint target) {
        return send(new VoteResponseOp(groupId, response), target);
    }

    @Override
    public boolean send(AppendRequest request, RaftEndpoint target) {
        return send(new AppendRequestOp(groupId, request), target);
    }

    @Override
    public boolean send(AppendSuccessResponse response, RaftEndpoint target) {
        return send(new AppendSuccessResponseOp(groupId, response), target);
    }

    @Override
    public boolean send(AppendFailureResponse response, RaftEndpoint target) {
        return send(new AppendFailureResponseOp(groupId, response), target);
    }

    @Override
    public boolean send(InstallSnapshot request, RaftEndpoint target) {
        return send(new InstallSnapshotOp(groupId, request), target);
    }

    @Override
    public boolean send(TriggerLeaderElection request, RaftEndpoint target) {
        return send(new TriggerLeaderElectionOp(groupId, request), target);
    }

    @Override
    public Object runOperation(Object op, long commitIndex) {
        RaftOp operation = (RaftOp) op;
        operation.setNodeEngine(nodeEngine);
        try {
            return operation.run(groupId, commitIndex);
        } catch (Throwable t) {
            operation.logFailure(t);
            return t;
        }
    }

    @Override
    public Object takeSnapshot(long commitIndex) {
        try {
            List<RestoreSnapshotOp> snapshotOps = new ArrayList<>();
            for (ServiceInfo serviceInfo : nodeEngine.getServiceInfos(SnapshotAwareService.class)) {
                SnapshotAwareService service = serviceInfo.getService();
                Object snapshot = service.takeSnapshot(groupId, commitIndex);
                if (snapshot != null) {
                    snapshotOps.add(new RestoreSnapshotOp(serviceInfo.getName(), snapshot));
                }
            }

            return snapshotOps;
        } catch (Throwable t) {
            return t;
        }
    }

    @Override
    public void restoreSnapshot(Object op, long commitIndex) {
        ILogger logger = nodeEngine.getLogger(getClass());
        List<RestoreSnapshotOp> snapshotOps = (List<RestoreSnapshotOp>) op;
        for (RestoreSnapshotOp snapshotOp : snapshotOps) {
            Object result = runOperation(snapshotOp, commitIndex);
            if (result instanceof Throwable) {
                logger.severe("Restore of " + snapshotOp + " failed...", (Throwable) result);
            }
        }
    }

    private boolean send(AsyncRaftOp operation, RaftEndpoint target) {
        CPMember targetMember = getCPMember(target);
        if (targetMember == null || localAddress.equals(targetMember.getAddress())) {
            if (localCPMember.getUuid().equals(target.getUuid())) {
                throw new IllegalStateException("Cannot send " + operation + " to "
                        + target + " because it's same with the local CP member!");
            }

            return false;
        }

        operation.setTargetEndpoint(target).setPartitionId(partitionId);
        return operationService.send(operation, targetMember.getAddress());
    }

    @Override
    public CPMember getCPMember(RaftEndpoint target) {
        return invocationManager.getCPMember(target);
    }

    @Override
    public void onNodeStatusChange(RaftNodeStatus status) {
        if (status == TERMINATED) {
            Collection<RaftNodeLifecycleAwareService> services = nodeEngine.getServices(RaftNodeLifecycleAwareService.class);
            for (RaftNodeLifecycleAwareService service : services) {
                service.onRaftNodeTerminated(groupId);
            }
        } else if (status == STEPPED_DOWN) {
            Collection<RaftNodeLifecycleAwareService> services = nodeEngine.getServices(RaftNodeLifecycleAwareService.class);
            for (RaftNodeLifecycleAwareService service : services) {
                service.onRaftNodeSteppedDown(groupId);
            }
        }
    }

    @Override
    public void onGroupDestroyed(CPGroupId groupId) {
        RaftService raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        nodeEngine.getExecutionService().execute(CP_SUBSYSTEM_EXECUTOR, () -> raftService.terminateRaftNode(groupId, true));

        Collection<RaftNodeLifecycleAwareService> services = nodeEngine.getServices(RaftNodeLifecycleAwareService.class);
        for (RaftNodeLifecycleAwareService service : services) {
            if (service == raftService) {
                continue;
            }
            service.onRaftNodeTerminated(groupId);
        }
    }
}
