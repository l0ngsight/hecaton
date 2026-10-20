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
import uk.org.hecaton.client.impl.protocol.codec.ScheduledExecutorSubmitToPartitionCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractPartitionMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService;
import uk.org.hecaton.scheduledexecutor.impl.TaskDefinition;
import uk.org.hecaton.scheduledexecutor.impl.operations.ScheduleTaskOperation;
import uk.org.hecaton.security.SecurityContext;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.ScheduledExecutorPermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import javax.security.auth.Subject;
import java.security.Permission;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class ScheduledExecutorSubmitToPartitionMessageTask
        extends AbstractPartitionMessageTask<ScheduledExecutorSubmitToPartitionCodec.RequestParameters> {

    public ScheduledExecutorSubmitToPartitionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
        setNamespaceAware();
    }

    @Override
    protected Operation prepareOperation() {
        Callable<?> callable = getCallable();
        SecurityContext securityContext = clientEngine.getSecurityContext();
        if (securityContext != null) {
            Subject subject = endpoint.getSubject();
            callable = securityContext.createSecureCallable(subject, callable);
            serializationService.getManagedContext().initialize(callable);
        }

        TaskDefinition<?> def = getTaskDefinition(callable);
        return new ScheduleTaskOperation(parameters.schedulerName, def);
    }

    @Override
    protected ScheduledExecutorSubmitToPartitionCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ScheduledExecutorSubmitToPartitionCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ScheduledExecutorSubmitToPartitionCodec.encodeResponse();
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
        return SecurityInterceptorConstants.SCHEDULE_ON_PARTITION;
    }

    private Callable<?> getCallable() {
        return serializationService.toObject(parameters.task);
    }

    private TaskDefinition<?> getTaskDefinition(Callable<?> callable) {
        return new TaskDefinition<>(TaskDefinition.Type.getById(parameters.type), parameters.taskName, callable,
                parameters.initialDelayInMillis, parameters.periodInMillis, TimeUnit.MILLISECONDS, isAutoDisposable());
    }

    @Override
    public Object[] getParameters() {
        Callable<?> callable = getCallable();
        TaskDefinition<?> def = getTaskDefinition(callable);
        return new Object[] { parameters.schedulerName, def };
    }

    private boolean isAutoDisposable() {
        return parameters.isAutoDisposableExists && parameters.autoDisposable;
    }

    @Override
    protected String getUserCodeNamespace() {
        return DistributedScheduledExecutorService.lookupNamespace(nodeEngine, getDistributedObjectName());
    }
}
