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

package uk.org.hecaton.spi.impl.operationservice.impl;

import uk.org.hecaton.client.impl.operations.OperationFactoryWrapper;
import uk.org.hecaton.client.impl.protocol.task.MessageTask;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.NodeState;
import uk.org.hecaton.instance.impl.OutOfMemoryErrorDispatcher;
import uk.org.hecaton.internal.hotrestart.InternalHotRestartService;
import uk.org.hecaton.internal.metrics.ExcludedMetricTargets;
import uk.org.hecaton.internal.metrics.MetricDescriptor;
import uk.org.hecaton.internal.metrics.MetricsRegistry;
import uk.org.hecaton.internal.metrics.Probe;
import uk.org.hecaton.internal.metrics.StaticMetricsProvider;
import uk.org.hecaton.internal.nio.Packet;
import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.partition.PartitionReplica;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.impl.SerializationServiceV1;
import uk.org.hecaton.internal.server.ServerConnection;
import uk.org.hecaton.internal.util.ExceptionUtil;
import uk.org.hecaton.internal.util.LatencyDistribution;
import uk.org.hecaton.internal.util.counters.Counter;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.serialization.HecatonSerializationException;
import uk.org.hecaton.spi.exception.CallerNotMemberException;
import uk.org.hecaton.spi.exception.PartitionMigratingException;
import uk.org.hecaton.spi.exception.ResponseAlreadySentException;
import uk.org.hecaton.spi.exception.RetryableException;
import uk.org.hecaton.spi.exception.RetryableHecatonException;
import uk.org.hecaton.spi.exception.WrongTargetException;
import uk.org.hecaton.spi.impl.AllowedDuringPassiveState;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.operationexecutor.OperationRunner;
import uk.org.hecaton.spi.impl.operationservice.BlockingOperation;
import uk.org.hecaton.spi.impl.operationservice.CallStatus;
import uk.org.hecaton.spi.impl.operationservice.Notifier;
import uk.org.hecaton.spi.impl.operationservice.Offload;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationResponseHandler;
import uk.org.hecaton.spi.impl.operationservice.ReadonlyOperation;
import uk.org.hecaton.spi.impl.operationservice.impl.operations.Backup;
import uk.org.hecaton.spi.impl.operationservice.impl.operations.PartitionIteratingOperation;
import uk.org.hecaton.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import uk.org.hecaton.spi.impl.operationservice.impl.responses.ErrorResponse;
import uk.org.hecaton.spi.impl.operationservice.impl.responses.NormalResponse;
import uk.org.hecaton.splitbrainprotection.SplitBrainProtectionException;
import uk.org.hecaton.splitbrainprotection.impl.SplitBrainProtectionServiceImpl;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.OPERATION_DISCRIMINATOR_GENERICID;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.OPERATION_DISCRIMINATOR_PARTITIONID;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_OPERATION_RUNNER_EXECUTED_OPERATIONS_COUNT;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.OPERATION_PREFIX_ADHOC;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.OPERATION_PREFIX_GENERIC;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.OPERATION_PREFIX_PARTITION;
import static uk.org.hecaton.internal.metrics.MetricTarget.MANAGEMENT_CENTER;
import static uk.org.hecaton.internal.metrics.ProbeLevel.DEBUG;
import static uk.org.hecaton.internal.util.counters.MwCounter.newMwCounter;
import static uk.org.hecaton.internal.util.counters.SwCounter.newSwCounter;
import static uk.org.hecaton.spi.impl.operationservice.CallStatus.OFFLOAD_ORDINAL;
import static uk.org.hecaton.spi.impl.operationservice.CallStatus.RESPONSE_ORDINAL;
import static uk.org.hecaton.spi.impl.operationservice.CallStatus.VOID_ORDINAL;
import static uk.org.hecaton.spi.impl.operationservice.CallStatus.WAIT_ORDINAL;
import static uk.org.hecaton.spi.impl.operationservice.OperationAccessor.setCallerAddress;
import static uk.org.hecaton.spi.impl.operationservice.OperationAccessor.setConnection;
import static uk.org.hecaton.spi.impl.operationservice.OperationResponseHandlerFactory.createEmptyResponseHandler;
import static uk.org.hecaton.spi.impl.operationservice.Operations.isJoinOperation;
import static uk.org.hecaton.spi.impl.operationservice.Operations.isMigrationOperation;
import static uk.org.hecaton.spi.impl.operationservice.Operations.isWanReplicationOperation;
import static uk.org.hecaton.spi.properties.ClusterProperty.DISABLE_STALE_READ_ON_PARTITION_MIGRATION;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

/**
 * Responsible for processing an Operation.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
@ExcludedMetricTargets(MANAGEMENT_CENTER)
public class OperationRunnerImpl extends OperationRunner implements StaticMetricsProvider {

    static final int AD_HOC_PARTITION_ID = -2;

    final OperationServiceImpl operationService;
    @Probe(name = OPERATION_METRIC_OPERATION_RUNNER_EXECUTED_OPERATIONS_COUNT, level = DEBUG)
    final Counter executedOperationsCounter;

    private final ILogger logger;
    private final Node node;
    private final NodeEngineImpl nodeEngine;

    private final Address thisAddress;
    private final boolean staleReadOnMigrationEnabled;

    private final Counter failedBackupsCounter;
    private final OperationBackupHandler backupHandler;

    // has only meaning for metrics.
    private final int genericId;

    // This field doesn't need additional synchronization, since a partition-specific OperationRunner
    // will never be called concurrently.
    private InternalPartition internalPartition;

    private final OutboundResponseHandler outboundResponseHandler;

    private final ConcurrentMap<Class, LatencyDistribution> opLatencyDistributions;

    // When partitionId >= 0, it is a partition specific
    // when partitionId = -1, it is generic
    // when partitionId = -2, it is ad hoc
    // an ad-hoc OperationRunner can only process generic operations, but it can be shared between threads
    // and therefore the {@link OperationRunner#currentTask()} always returns null
    OperationRunnerImpl(OperationServiceImpl operationService,
                        int partitionId,
                        int genericId,
                        Counter failedBackupsCounter,
                        ConcurrentMap<Class, LatencyDistribution> opLatencyDistributions) {
        super(partitionId);
        this.genericId = genericId;
        this.operationService = operationService;
        this.logger = operationService.node.getLogger(OperationRunnerImpl.class);
        this.node = operationService.node;
        this.thisAddress = node.getThisAddress();
        this.nodeEngine = operationService.nodeEngine;
        this.outboundResponseHandler = operationService.outboundResponseHandler;
        this.staleReadOnMigrationEnabled = !node.getProperties().getBoolean(DISABLE_STALE_READ_ON_PARTITION_MIGRATION);
        this.failedBackupsCounter = failedBackupsCounter;
        this.backupHandler = operationService.backupHandler;
        this.opLatencyDistributions = opLatencyDistributions;
        // only an ad-hoc operation runner will be called concurrently
        this.executedOperationsCounter = partitionId == AD_HOC_PARTITION_ID ? newMwCounter() : newSwCounter();
    }

    public OperationBackupHandler getBackupHandler() {
        return backupHandler;
    }

    @Override
    public long executedOperationsCount() {
        return executedOperationsCounter.get();
    }

    @Override
    public void provideStaticMetrics(MetricsRegistry registry) {
        if (partitionId >= 0) {
            MetricDescriptor descriptor = registry.newMetricDescriptor()
                    .withPrefix(OPERATION_PREFIX_PARTITION)
                    .withDiscriminator(OPERATION_DISCRIMINATOR_PARTITIONID,
                            String.valueOf(partitionId));
            registry.registerStaticMetrics(descriptor, this);
        } else if (partitionId == -1) {
            MetricDescriptor descriptor = registry.newMetricDescriptor()
                    .withPrefix(OPERATION_PREFIX_GENERIC)
                    .withDiscriminator(OPERATION_DISCRIMINATOR_GENERICID,
                            String.valueOf(genericId));
            registry.registerStaticMetrics(descriptor, this);
        } else {
            registry.registerStaticMetrics(this, OPERATION_PREFIX_ADHOC);
        }
    }

    @Override
    public void run(Runnable task) {
        long startNanos = System.nanoTime();

        boolean publishCurrentTask = publishCurrentTask();

        if (publishCurrentTask) {
            currentTask = task;
        }

        try {
            task.run();
        } finally {
            if (publishCurrentTask) {
                currentTask = null;
            }

            record(task, startNanos);
        }
    }

    boolean publishCurrentTask() {
        boolean isClientRunnable = currentTask instanceof MessageTask;
        return getPartitionId() != AD_HOC_PARTITION_ID && (currentTask == null || isClientRunnable);
    }

    @Override
    public void run(Operation op) {
         run(op, System.nanoTime());
    }

    public boolean metWithPreconditions(Operation op) {
        checkNodeState(op);
        if (timeout(op)) {
            return false;
        }
        ensureNoPartitionProblems(op);
        ensureNoSplitBrain(op);
        return true;
    }

    /**
     * Like {@link #metWithPreconditions} but without timeout checks.
     * <p>
     * Includes checks related to node and cluster health
     * before execution of an operation.
     */
    public void ensureNodeAndClusterHealth(Operation op) {
        checkNodeState(op);
        ensureNoPartitionProblems(op);
        ensureNoSplitBrain(op);
    }

    /**
     * Runs the provided operation.
     *
     * @param op         the operation to execute
     * @param startNanos the time, as returned by {@link System#nanoTime} when this operation
     *                   started execution
     */
    protected void run(Operation op, long startNanos) {
        executedOperationsCounter.inc();

        boolean publishCurrentTask = publishCurrentTask();
        if (publishCurrentTask) {
            currentTask = op;
        }

        try {
            if (!metWithPreconditions(op)) {
                return;
            }

            op.beforeRun();
            call(op);
        } catch (Throwable e) {
            handleOperationError(op, e);
        } finally {
            op.afterRunFinal();
            if (publishCurrentTask) {
                currentTask = null;
            }
            record(op, startNanos);
        }
    }

    protected void record(Object op, long startNanos) {
        if (opLatencyDistributions != null) {
            Class<?> c = op.getClass();
            if (op instanceof PartitionIteratingOperation operation) {
                c = operation.getOperationFactory().getClass();
                if (operation.getOperationFactory() instanceof OperationFactoryWrapper wrapper) {
                    c = wrapper.getOperationFactory().getClass();
                }
            }

            LatencyDistribution distribution = opLatencyDistributions.get(c);
            // Note: we want to prevent lock here, if collision happened.
            if (distribution == null) {
                distribution = opLatencyDistributions.computeIfAbsent(c, k -> new LatencyDistribution());
            }
            distribution.recordNanos(System.nanoTime() - startNanos);
        }
    }

    void call(Operation op) throws Exception {
        CallStatus callStatus = op.call();

        switch (callStatus.ordinal()) {
            case RESPONSE_ORDINAL:
                sendBackupsAndResponse(op);
                afterRun(op);
                break;
            case VOID_ORDINAL:
                backupHandler.sendBackups(op);
                afterRun(op);
                break;
            case OFFLOAD_ORDINAL:
                op.afterRun();
                Offload offload = (Offload) callStatus;
                offload.init(nodeEngine, operationService.asyncOperations);
                offload.start();
                break;
            case WAIT_ORDINAL:
                nodeEngine.getOperationParker().park((BlockingOperation) op);
                break;
            default:
                throw new IllegalStateException();
        }
    }

    public void sendBackupsAndResponse(Operation op) {
        int backupAcks = backupHandler.sendBackups(op);
        Object response = op.getResponse();
        if (backupAcks > 0) {
            response = new NormalResponse(response, op.getCallId(), backupAcks, op.isUrgent());
        }
        try {
            op.sendResponse(response);
        } catch (ResponseAlreadySentException e) {
            logOperationError(op, e);
        }
    }

    private void checkNodeState(Operation op) {
        NodeState state = node.getState();
        if (state == NodeState.ACTIVE) {
            return;
        }

        Address localAddress = node.getThisAddress();
        if (state == NodeState.SHUT_DOWN) {
            throw new HecatonInstanceNotActiveException("Member " + localAddress + " is shut down! Operation: " + op);
        }

        if (op instanceof AllowedDuringPassiveState) {
            return;
        }

        // Cluster is in passive state. There is no need to retry.
        if (nodeEngine.getClusterService().getClusterState() == ClusterState.PASSIVE) {
            throw new IllegalStateException("Cluster is in " + ClusterState.PASSIVE + " state! Operation: " + op);
        }

        InternalHotRestartService hotRestartService = node.getNodeExtension().getInternalHotRestartService();
        if (hotRestartService.isEnabled() && !hotRestartService.isStartCompleted()) {
            throw new RetryableHecatonException("Recovery from persistence is still in progress. Operation: " + op);
        }
    }

    /**
     * Ensures that there is no split brain if the split brain protection is configured and the operation
     * service is split brain protection aware.
     *
     * @param op the operation for which the minimum cluster size property must satisfy
     * @throws SplitBrainProtectionException if the operation requires a split brain protection and
     *                                       the minimum cluster size property is not satisfied
     */
    private void ensureNoSplitBrain(Operation op) {
        SplitBrainProtectionServiceImpl splitBrainProtectionService =
                operationService.nodeEngine.getSplitBrainProtectionService();
        splitBrainProtectionService.ensureNoSplitBrain(op);
    }

    public boolean timeout(Operation op) {
        if (!operationService.isCallTimedOut(op)) {
            return false;
        }

        op.sendResponse(new CallTimeoutResponse(op.getCallId(), op.isUrgent()));
        return true;
    }

    private void afterRun(Operation op) {
        try {
            op.afterRun();
            if (op instanceof Notifier notifier) {
                if (notifier.shouldNotify()) {
                    operationService.nodeEngine.getOperationParker().unpark(notifier);
                }
            }
        } catch (Throwable e) {
            // passed the response phase
            // `afterRun` and `notifier` errors cannot be sent to the caller anymore
            // just log the error
            logOperationError(op, e);
        }
    }

    private void ensureNoPartitionProblems(Operation op) {
        int partitionId = op.getPartitionId();

        if (partitionId < 0) {
            return;
        }

        if (partitionId != getPartitionId()) {
            throw new IllegalStateException("wrong partition, expected: " + getPartitionId() + " but found:" + partitionId);
        }

        if (internalPartition == null) {
            internalPartition = nodeEngine.getPartitionService().getPartition(partitionId);
        }

        if (!isAllowedToExecuteDuringMigration(op) && internalPartition.isMigrating()) {
            throw new PartitionMigratingException(thisAddress, partitionId,
                    op.getClass().getName(), op.getServiceName());
        }

        PartitionReplica owner = internalPartition.getReplica(op.getReplicaIndex());
        if (op.validatesTarget() && (owner == null || !owner.isIdentical(node.getLocalMember()))) {
            Member target = owner != null ? node.getClusterService().getMember(owner.address(), owner.uuid()) : null;
            throw new WrongTargetException(node.getLocalMember(), target, partitionId, op.getReplicaIndex(),
                    op.getClass().getName(), op.getServiceName());
        }
    }

    private boolean isAllowedToExecuteDuringMigration(Operation op) {
        return (op instanceof ReadonlyOperation && staleReadOnMigrationEnabled) || isMigrationOperation(op);
    }

    public void handleOperationError(Operation operation, Throwable e) {
        if (e instanceof OutOfMemoryError error) {
            OutOfMemoryErrorDispatcher.onOutOfMemory(error);
        }
        try {
            operation.onExecutionFailure(e);
        } catch (Throwable t) {
            logger.warning("While calling 'operation.onFailure(e)'... op: " + operation + ", error: " + e, t);
        }

        operation.logError(e);

        if (operation instanceof Backup) {
            failedBackupsCounter.inc();
            return;
        }

        // A response is sent regardless of the Operation.returnsResponse method because some operations do want to send
        // back a response, but they didn't want to send it yet, but they ran into some kind of error. If on the receiving
        // side no invocation is waiting, the response is ignored.
        sendResponseAfterOperationError(operation, e);
    }

    private void sendResponseAfterOperationError(Operation operation, Throwable e) {
        try {
            if (node.getState() != NodeState.SHUT_DOWN) {
                operation.sendResponse(e);
            } else if (operation.executedLocally()) {
                operation.sendResponse(new HecatonInstanceNotActiveException());
            }
        } catch (Throwable t) {
            logger.warning("While sending op error... op: " + operation + ", error: " + e, t);
        }
    }

    private static void logOperationError(Operation op, Throwable e) {
        if (e instanceof OutOfMemoryError error) {
            OutOfMemoryErrorDispatcher.onOutOfMemory(error);
        }
        op.logError(e);
    }

    @Override
    public void run(Packet packet) throws Exception {
        long startNanos = System.nanoTime();
        boolean publishCurrentTask = publishCurrentTask();

        if (publishCurrentTask) {
            currentTask = packet;
        }

        ServerConnection connection = packet.getConn();
        Address caller = connection.getRemoteAddress();
        UUID callerUuid = connection.getRemoteUuid();
        Operation op = null;
        try {
            Object object = nodeEngine.toObject(packet);
            op = (Operation) object;
            op.setNodeEngine(nodeEngine);
            setCallerAddress(op, caller);
            setConnection(op, connection);
            setCallerUuidIfNotSet(op, callerUuid);
            setOperationResponseHandler(op);

            if (!ensureValidMember(op)) {
                return;
            }

            if (publishCurrentTask) {
                currentTask = null;
            }
            run(op, startNanos);
        } catch (Throwable throwable) {
            // If exception happens we need to extract the callId from the bytes directly!
            long callId = extractOperationCallId(packet);
            outboundResponseHandler.send(connection.getConnectionManager(), caller,
                    new ErrorResponse(throwable, callId, packet.isUrgent()));
            logOperationDeserializationException(throwable, callId);
            throw ExceptionUtil.rethrow(throwable);
        } finally {
            if (op != null) {
                op.clearThreadContext();
            }
            if (publishCurrentTask) {
                currentTask = null;
            }
        }
    }

    /**
     * This method has a direct dependency on how objects are serialized.
     * If the stream format is changed, this extraction method must be changed as well.
     * <p>
     * It makes an assumption that the callId is the first long field in the serialized operation.
     */
    private long extractOperationCallId(Data data) throws IOException {
        ObjectDataInput input = ((SerializationServiceV1) node.getSerializationService())
                .initDataSerializableInputAndSkipTheHeader(data);
        return input.readLong();
    }

    private void setOperationResponseHandler(Operation op) {
        OperationResponseHandler handler = outboundResponseHandler;
        if (op.getCallId() == 0) {
            if (op.returnsResponse()) {
                throw new HecatonException(
                        "Operation " + op + " wants to return a response, but doesn't have a call ID");
            }
            handler = createEmptyResponseHandler();
        }
        op.setOperationResponseHandler(handler);
    }

    private boolean ensureValidMember(Operation op) {
        if (node.clusterService.getMember(op.getCallerAddress()) != null
                || isJoinOperation(op)
                || isWanReplicationOperation(op)) {
            return true;
        }

        Exception error = new CallerNotMemberException(thisAddress, op.getCallerAddress(), op.getPartitionId(),
                op.getClass().getName(), op.getServiceName());
        handleOperationError(op, error);
        return false;
    }

    private void setCallerUuidIfNotSet(Operation op, UUID callerUuid) {
        if (op.getCallerUuid() != null) {
            return;
        }
        if (callerUuid != null) {
            op.setCallerUuid(callerUuid);
        }
    }

    private void logOperationDeserializationException(Throwable t, long callId) {
        boolean returnsResponse = callId != 0;

        if (t instanceof RetryableException) {
            final Level level = returnsResponse ? FINEST : WARNING;
            if (logger.isLoggable(level)) {
                logger.log(level, t.getClass().getName() + ": " + t.getMessage());
            }
        } else if (t instanceof OutOfMemoryError) {
            try {
                logException(t.getMessage(), t, SEVERE);
            } catch (Throwable ignored) {
                logger.severe(ignored.getMessage(), t);
            }
        } else if (t instanceof HecatonSerializationException) {
            if (!node.getClusterService().isJoined()) {
                logException("A serialization exception occurred while joining a cluster, is this member compatible with "
                        + "other members of the cluster?", t, SEVERE);
            } else {
                logException(t.getMessage(), t, nodeEngine.isRunning() ? SEVERE : FINEST);
            }
        } else {
            logException(t.getMessage(), t, nodeEngine.isRunning() ? SEVERE : FINEST);
        }
    }

    private void logException(String message, Throwable t, Level level) {
        if (logger.isLoggable(level)) {
            logger.log(level, message, t);
        }
    }

}
