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

package uk.org.hecaton.client.impl.protocol.task.scheduledexecutor;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorGetAllScheduledFuturesCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractMessageTask;
import uk.org.hecaton.client.impl.protocol.task.BlockingMessageTask;
import uk.org.hecaton.cluster.impl.MemberImpl;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.scheduledexecutor.ScheduledTaskHandler;
import uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService;
import uk.org.hecaton.scheduledexecutor.impl.InvokeOnMembers;
import uk.org.hecaton.scheduledexecutor.impl.ScheduledTaskHandlerAccessor;
import uk.org.hecaton.scheduledexecutor.impl.operations.GetAllScheduledOnMemberOperation;
import uk.org.hecaton.scheduledexecutor.impl.operations.GetAllScheduledOnPartitionOperationFactory;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.ScheduledExecutorPermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;

public class ScheduledExecutorGetAllScheduledMessageTask
        extends AbstractMessageTask<String>
        implements BlockingMessageTask {

    public ScheduledExecutorGetAllScheduledMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        List<ScheduledTaskHandler> scheduledTasks = new LinkedList<>();
        retrieveAllMemberOwnedScheduled(scheduledTasks);
        retrieveAllPartitionOwnedScheduled(scheduledTasks);
        sendResponse(scheduledTasks);
    }

    @Override
    protected String decodeClientMessage(ClientMessage clientMessage) {
        return ScheduledExecutorGetAllScheduledFuturesCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ScheduledExecutorGetAllScheduledFuturesCodec
                .encodeResponse((Collection<ScheduledTaskHandler>) response);
    }

    @Override
    public String getServiceName() {
        return DistributedScheduledExecutorService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ScheduledExecutorPermission(parameters, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.GET_ALL_SCHEDULED_FUTURES;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters};
    }

    private void retrieveAllMemberOwnedScheduled(List<ScheduledTaskHandler> accumulator) {
        try {
            InvokeOnMembers invokeOnMembers = new InvokeOnMembers(nodeEngine, getServiceName(),
                    new GetAllScheduledOnMemberOperationFactory(parameters),
                    nodeEngine.getClusterService().getMembers());
            accumulateTaskHandlersAsUrnValues(accumulator, invokeOnMembers.invoke());
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private void retrieveAllPartitionOwnedScheduled(List<ScheduledTaskHandler> accumulator) {
        try {
            accumulateTaskHandlersAsUrnValues(accumulator, nodeEngine.getOperationService().invokeOnAllPartitions(
                    getServiceName(), new GetAllScheduledOnPartitionOperationFactory(parameters)));
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @SuppressWarnings("unchecked")
    private void accumulateTaskHandlersAsUrnValues(List<ScheduledTaskHandler> accumulator,
                                                   Map<?, ?> taskHandlersMap) {

        ClusterService clusterService = nodeEngine.getClusterService();
        IPartitionService partitionService = nodeEngine.getPartitionService();

        for (Map.Entry<?, ?> entry : taskHandlersMap.entrySet()) {
            MemberImpl owner;
            Object key = entry.getKey();
            if (key instanceof Number) {
                owner = clusterService.getMember(partitionService.getPartitionOwner((Integer) key));
            } else {
                owner = (MemberImpl) key;
            }
            List<ScheduledTaskHandler> handlers = (List<ScheduledTaskHandler>) entry.getValue();
            for (ScheduledTaskHandler handler : handlers) {
                ScheduledTaskHandlerAccessor.setUuid(handler, owner.getUuid());
            }
            accumulator.addAll(handlers);
        }
    }

    private class GetAllScheduledOnMemberOperationFactory implements Supplier<Operation> {

        private final String schedulerName;

        GetAllScheduledOnMemberOperationFactory(String schedulerName) {
            this.schedulerName = schedulerName;
        }

        @Override
        public Operation get() {
            return new GetAllScheduledOnMemberOperation(schedulerName)
                    .setCallerUuid(endpoint.getUuid());
        }
    }
}
