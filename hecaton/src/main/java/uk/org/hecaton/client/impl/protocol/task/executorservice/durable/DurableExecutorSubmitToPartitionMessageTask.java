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

package uk.org.hecaton.client.impl.protocol.task.executorservice.durable;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.DurableExecutorSubmitToPartitionCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractPartitionMessageTask;
import uk.org.hecaton.config.DurableExecutorConfig;
import uk.org.hecaton.durableexecutor.impl.operations.TaskOperation;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.security.SecurityContext;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.DurableExecutorServicePermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import javax.security.auth.Subject;
import java.security.Permission;
import java.util.concurrent.Callable;

import static uk.org.hecaton.durableexecutor.impl.DistributedDurableExecutorService.SERVICE_NAME;

public class DurableExecutorSubmitToPartitionMessageTask
        extends AbstractPartitionMessageTask<DurableExecutorSubmitToPartitionCodec.RequestParameters> {

    public DurableExecutorSubmitToPartitionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
        setNamespaceAware();
    }

    @Override
    protected Operation prepareOperation() {
        SecurityContext securityContext = clientEngine.getSecurityContext();
        Data callableData = parameters.callable;
        if (securityContext != null) {
            Subject subject = endpoint.getSubject();
            Object taskObject = serializationService.toObject(parameters.callable);
            Callable callable;
            if (taskObject instanceof Runnable runnable) {
                callable = securityContext.createSecureCallable(subject, runnable);
            } else {
                callable = securityContext.createSecureCallable(subject, (Callable<? extends Object>) taskObject);
            }
            callableData = serializationService.toData(callable);
        }
        return new TaskOperation(parameters.name, callableData);
    }


    @Override
    protected DurableExecutorSubmitToPartitionCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return DurableExecutorSubmitToPartitionCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return DurableExecutorSubmitToPartitionCodec.encodeResponse((Integer) response);
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new DurableExecutorServicePermission(parameters.name, ActionConstants.ACTION_MODIFY);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.SUBMIT_TO_PARTITION;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    protected String getUserCodeNamespace() {
        DurableExecutorConfig config = nodeEngine.getConfig().findDurableExecutorConfig(parameters.name);
        return config == null ? null : config.getUserCodeNamespace();
    }
}
