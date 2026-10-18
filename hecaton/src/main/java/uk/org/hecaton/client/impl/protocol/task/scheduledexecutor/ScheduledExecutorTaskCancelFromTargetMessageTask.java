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
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorCancelFromMemberCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractTargetMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.scheduledexecutor.ScheduledTaskHandler;
import uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService;
import uk.org.hecaton.scheduledexecutor.impl.ScheduledTaskHandlerImpl;
import uk.org.hecaton.scheduledexecutor.impl.operations.CancelTaskOperation;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.ScheduledExecutorPermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.UUID;

public class ScheduledExecutorTaskCancelFromTargetMessageTask
        extends AbstractTargetMessageTask<ScheduledExecutorCancelFromMemberCodec.RequestParameters> {

    public ScheduledExecutorTaskCancelFromTargetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        ScheduledTaskHandler handler = ScheduledTaskHandlerImpl.of(parameters.memberUuid,
                parameters.schedulerName,
                parameters.taskName);
        return new CancelTaskOperation(handler, parameters.mayInterruptIfRunning);
    }

    @Override
    protected UUID getTargetUuid() {
        return parameters.memberUuid;
    }

    @Override
    protected ScheduledExecutorCancelFromMemberCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ScheduledExecutorCancelFromMemberCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ScheduledExecutorCancelFromMemberCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return DistributedScheduledExecutorService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ScheduledExecutorPermission(parameters.schedulerName, ActionConstants.ACTION_MODIFY);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.schedulerName;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.CANCEL;
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.mayInterruptIfRunning};
    }
}
