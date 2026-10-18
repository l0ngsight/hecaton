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

package uk.org.hecaton.internal.cluster.impl.operations;

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.OnJoinPermissionOperationName;
import uk.org.hecaton.config.SecurityConfig;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.cluster.MemberInfo;
import uk.org.hecaton.internal.cluster.impl.ClusterDataSerializerHook;
import uk.org.hecaton.internal.cluster.impl.ClusterJoinManager;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.internal.management.operation.UpdatePermissionConfigOperation;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationAccessor;
import uk.org.hecaton.spi.impl.operationservice.OperationResponseHandler;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.spi.impl.operationservice.TargetAware;
import uk.org.hecaton.spi.impl.operationservice.UrgentSystemOperation;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import static uk.org.hecaton.internal.serialization.impl.SerializationUtil.readCollection;
import static uk.org.hecaton.internal.serialization.impl.SerializationUtil.writeCollection;
import static uk.org.hecaton.spi.impl.operationexecutor.OperationRunner.runDirect;
import static uk.org.hecaton.spi.impl.operationservice.OperationResponseHandlerFactory.createErrorLoggingResponseHandler;
import static uk.org.hecaton.internal.util.Preconditions.checkNegative;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;

public class OnJoinOp
        extends AbstractJoinOperation implements UrgentSystemOperation, TargetAware {

    private Collection<Operation> operations;

    public OnJoinOp() {
    }

    public OnJoinOp(Collection<Operation> ops) {
        for (Operation op : ops) {
            checkNotNull(op, "op can't be null");
            checkNegative(op.getPartitionId(), "Post join operation can not have a partition ID!");
        }
        operations = ops;
    }

    @Override
    public String getServiceName() {
        return ClusterServiceImpl.SERVICE_NAME;
    }

    @Override
    public void beforeRun() throws Exception {
        if (!operations.isEmpty()) {
            NodeEngine nodeEngine = getNodeEngine();
            OperationResponseHandler responseHandler = createErrorLoggingResponseHandler(getLogger());
            for (Operation op : operations) {
                op.setNodeEngine(nodeEngine);
                op.setOperationResponseHandler(responseHandler);
                OperationAccessor.setCallerAddress(op, getCallerAddress());
                OperationAccessor.setConnection(op, getConnection());
            }
        }
    }

    @Override
    public void run() throws Exception {
        run(null);
    }

    /**
     * Used in {@link ClusterJoinManager#startJoin(MemberInfo, OnJoinOp)} to prevent
     * quadratic complexity during batched cluster joining
     *
     * @param skippedForBroadcast Set of {@link Address} to skip broadcasting
     *                            to if we are master of a cluster
     */
    public void runWithoutBroadcastTo(Set<Address> skippedForBroadcast) {
        run(skippedForBroadcast);
    }

    private void run(Set<Address> skippedForBroadcast) {
        if (!operations.isEmpty()) {
            SecurityConfig securityConfig = getNodeEngine().getConfig().getSecurityConfig();
            boolean runPermissionUpdates = securityConfig.getOnJoinPermissionOperation() == OnJoinPermissionOperationName.RECEIVE;
            for (Operation op : operations) {
                if ((op instanceof UpdatePermissionConfigOperation) && !runPermissionUpdates) {
                    continue;
                }
                try {
                    // not running via OperationService since we don't want any restrictions like cluster state check etc.
                    runDirect(op);
                } catch (Exception e) {
                    getLogger().warning("Error while running post-join operation: " + op, e);
                }
            }

            final ClusterService clusterService = getService();
            // if executed on master, broadcast to all other members except sender (joining member)
            if (clusterService.isMaster()) {
                final OperationService operationService = getNodeEngine().getOperationService();
                for (Member member : clusterService.getMembers()) {
                    if (skippedForBroadcast != null && skippedForBroadcast.contains(member.getAddress())) {
                        continue;
                    }
                    if (!member.localMember() && !member.getUuid().equals(getCallerUuid())) {
                        OnJoinOp operation = new OnJoinOp(operations);
                        operationService.invokeOnTarget(getServiceName(), operation, member.getAddress());
                    }
                }
            }
        }
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        for (Operation op : operations) {
            onOperationFailure(op, e);
        }
    }

    private void onOperationFailure(Operation op, Throwable e) {
        try {
            op.onExecutionFailure(e);
        } catch (Throwable t) {
            getLogger().warning("While calling operation.onFailure(). op: " + op, t);
        }
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        writeCollection(operations, out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        operations = readCollection(in);
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", operations=").append(operations);
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.POST_JOIN;
    }

    @Override
    public void setTarget(Address address) {
        for (Operation op : operations) {
            if (op instanceof TargetAware aware) {
                aware.setTarget(address);
            }
        }
    }
}
