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

package uk.org.hecaton.internal.partition.impl;

import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.partition.FragmentedMigrationAwareService;
import uk.org.hecaton.internal.partition.NonFragmentedServiceNamespace;
import uk.org.hecaton.internal.partition.PartitionReplica;
import uk.org.hecaton.internal.partition.PartitionReplicationEvent;
import uk.org.hecaton.internal.partition.operation.PartitionBackupReplicaAntiEntropyOperation;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.PartitionSpecificRunnable;
import uk.org.hecaton.spi.impl.executionservice.ExecutionService;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.spi.impl.operationservice.UrgentSystemOperation;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

import static uk.org.hecaton.internal.partition.IPartitionService.SERVICE_NAME;

public abstract class AbstractPartitionPrimaryReplicaAntiEntropyTask
        implements PartitionSpecificRunnable, UrgentSystemOperation {

    private static final int OPERATION_TRY_COUNT = 10;

    private static final int OPERATION_TRY_PAUSE_MILLIS = 250;

    protected final NodeEngine nodeEngine;

    protected final InternalPartitionServiceImpl partitionService;

    protected final int partitionId;

    public AbstractPartitionPrimaryReplicaAntiEntropyTask(NodeEngine nodeEngine, int partitionId) {
        this.nodeEngine = nodeEngine;
        this.partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        this.partitionId = partitionId;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    // works only on primary. backups are retained
    // in PartitionBackupReplicaAntiEntropyTask
    final Collection<ServiceNamespace> retainAndGetNamespaces() {
        PartitionReplicationEvent event = new PartitionReplicationEvent(null, partitionId, 0);
        Collection<FragmentedMigrationAwareService> services
                = nodeEngine.getServices(FragmentedMigrationAwareService.class);

        Collection<ServiceNamespace> namespaces = new HashSet<>();
        for (FragmentedMigrationAwareService service : services) {
            Collection<ServiceNamespace> serviceNamespaces = service.getAllServiceNamespaces(event);
            if (serviceNamespaces != null) {
                namespaces.addAll(serviceNamespaces);
            }
        }
        namespaces.add(NonFragmentedServiceNamespace.INSTANCE);

        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
        replicaManager.retainNamespaces(partitionId, namespaces);

        ILogger logger = nodeEngine.getLogger(getClass());
        if (logger.isFinestEnabled()) {
            logger.finest("Retained namespaces for partitionId=" + partitionId + ". Service namespaces="
                    + namespaces + ", retained namespaces=" + replicaManager.getNamespaces(partitionId));
        }
        return replicaManager.getNamespaces(partitionId);
    }

    final void invokePartitionBackupReplicaAntiEntropyOp(int replicaIndex, PartitionReplica target,
                                                         Collection<ServiceNamespace> namespaces,
                                                         BiConsumer<Object, Throwable> callback) {
        if (skipSendingToTarget(target)) {
            return;
        }

        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
        ConcurrentMap<ServiceNamespace, Long> versionMap = new ConcurrentHashMap<>();
        for (ServiceNamespace ns : namespaces) {
            long[] versions = replicaManager.getPartitionReplicaVersions(partitionId, ns);
            long currentReplicaVersion = versions[replicaIndex - 1];
            versionMap.put(ns, currentReplicaVersion);
        }

        boolean hasCallback = (callback != null);

        Operation op = new PartitionBackupReplicaAntiEntropyOperation(versionMap, hasCallback);
        op.setPartitionId(partitionId)
                .setReplicaIndex(replicaIndex)
                .setServiceName(SERVICE_NAME);

        ILogger logger = nodeEngine.getLogger(getClass());
        if (logger.isFinestEnabled()) {
            logger.finest("Sending anti-entropy operation to " + target + " for partitionId=" + partitionId
                    + ", replicaIndex=" + replicaIndex + ", namespaces=" + versionMap);
        }

        OperationService operationService = nodeEngine.getOperationService();
        if (hasCallback) {
            ExecutorService asyncExecutor =
                    nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR);
            operationService.createInvocationBuilder(SERVICE_NAME, op, target.address())
                    .setTryCount(OPERATION_TRY_COUNT)
                    .setTryPauseMillis(OPERATION_TRY_PAUSE_MILLIS)
                    .invoke()
                    .whenCompleteAsync(callback, asyncExecutor);
        } else {
            operationService.send(op, target.address());
        }
    }

    private boolean skipSendingToTarget(PartitionReplica target) {
        ClusterService clusterService = nodeEngine.getClusterService();

        assert !target.isIdentical(nodeEngine.getLocalMember()) : "Could not send anti-entropy operation, because "
                + target + " is local member itself! Local-member: " + clusterService.getLocalMember()
                + ", " + partitionService.getPartition(partitionId);

        if (clusterService.getMember(target.address(), target.uuid()) == null) {
            ILogger logger = nodeEngine.getLogger(getClass());
            if (logger.isFinestEnabled()) {
                if (clusterService.isMissingMember(target.address(), target.uuid())) {
                    logger.finest("Could not send anti-entropy operation, because %s is a missing member. %s", target,
                            partitionService.getPartition(partitionId));
                } else {
                    logger.finest("Could not send anti-entropy operation, because %s is not a known member. %s", target,
                            partitionService.getPartition(partitionId));
                }
            }
            return true;
        }
        return false;
    }
}
