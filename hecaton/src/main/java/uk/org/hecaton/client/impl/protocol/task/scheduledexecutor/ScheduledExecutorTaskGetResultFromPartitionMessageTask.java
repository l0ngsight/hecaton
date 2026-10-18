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
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorGetResultFromPartitionCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractPartitionMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.scheduledexecutor.ScheduledTaskHandler;
import uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService;
import uk.org.hecaton.scheduledexecutor.impl.ScheduledTaskHandlerImpl;
import uk.org.hecaton.scheduledexecutor.impl.ScheduledTaskResult;
import uk.org.hecaton.scheduledexecutor.impl.operations.GetResultOperation;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.ScheduledExecutorPermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;

public class ScheduledExecutorTaskGetResultFromPartitionMessageTask
        extends AbstractPartitionMessageTask<ScheduledExecutorGetResultFromPartitionCodec.RequestParameters> {

    public ScheduledExecutorTaskGetResultFromPartitionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        ScheduledTaskHandler handler = ScheduledTaskHandlerImpl.of(getPartitionId(),
                parameters.schedulerName,
                parameters.taskName);
        return new GetResultOperation(handler);
    }

    @Override
    protected ScheduledExecutorGetResultFromPartitionCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ScheduledExecutorGetResultFromPartitionCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        Data data = nodeEngine.getSerializationService().toData(response);
        return ScheduledExecutorGetResultFromPartitionCodec.encodeResponse(data);
    }

    @Override
    public String getServiceName() {
        return DistributedScheduledExecutorService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ScheduledExecutorPermission(parameters.schedulerName, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.schedulerName;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.GET_RESULT;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    /**
     * Exceptions may be wrapped in ExecutionExceptionDecorator, the wrapped ExecutionException should be sent to
     * the client.
     */
    @Override
    protected Throwable peelIfNeeded(Throwable throwable) {
        if (throwable instanceof ScheduledTaskResult.ExecutionExceptionDecorator) {
            throwable = throwable.getCause();
        }
        return super.peelIfNeeded(throwable);
    }

    @Override
    protected String getUserCodeNamespace() {
        // This task is not Namespace-aware so it doesn't matter
        return null;
    }
}
