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

package uk.org.hecaton.client.impl.protocol.task.executorservice;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.ExecutorServiceSubmitToMemberCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractTargetMessageTask;
import uk.org.hecaton.executor.impl.DistributedExecutorService;
import uk.org.hecaton.executor.impl.operations.MemberCallableTaskOperation;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.security.SecurityContext;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.ExecutorServicePermission;
import uk.org.hecaton.security.permission.UserCodeNamespacePermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import javax.security.auth.Subject;
import java.security.Permission;
import java.util.UUID;
import java.util.concurrent.Callable;

public class ExecutorServiceSubmitToAddressMessageTask
        extends AbstractTargetMessageTask<ExecutorServiceSubmitToMemberCodec.RequestParameters> {

    public ExecutorServiceSubmitToAddressMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected UUID getTargetUuid() {
        return parameters.memberUUID;
    }

    @Override
    protected Operation prepareOperation() {
        SecurityContext securityContext = clientEngine.getSecurityContext();
        Data callableData = parameters.callable;
        if (securityContext != null) {
            Subject subject = endpoint.getSubject();
            Object taskObject = NamespaceUtil.callWithNamespace(nodeEngine,
                    DistributedExecutorService.lookupNamespace(nodeEngine, parameters.name),
                    () -> serializationService.toObject(parameters.callable));
            Callable callable;
            if (taskObject instanceof Runnable runnable) {
                callable = securityContext.createSecureCallable(subject, runnable);
            } else {
                callable = securityContext.createSecureCallable(subject, (Callable<? extends Object>) taskObject);
            }
            callableData = serializationService.toData(callable);
        }

        MemberCallableTaskOperation op = new MemberCallableTaskOperation(parameters.name, parameters.uuid, callableData);
        op.setCallerUuid(endpoint.getUuid());
        return op;
    }

    @Override
    protected ExecutorServiceSubmitToMemberCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ExecutorServiceSubmitToMemberCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        Data data = serializationService.toData(response);
        return ExecutorServiceSubmitToMemberCodec.encodeResponse(data);
    }

    @Override
    public String getServiceName() {
        return DistributedExecutorService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new ExecutorServicePermission(parameters.name, ActionConstants.ACTION_MODIFY);
    }

    @Override
    public Permission getUserCodeNamespacePermission() {
        String namespace = DistributedExecutorService.lookupNamespace(nodeEngine, parameters.name);
        return namespace != null ? new UserCodeNamespacePermission(namespace, ActionConstants.ACTION_USE) : null;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.SUBMIT_TO_ADDRESS;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
