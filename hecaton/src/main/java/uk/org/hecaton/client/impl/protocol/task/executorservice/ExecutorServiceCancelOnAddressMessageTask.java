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
import uk.org.hecaton.client.impl.protocol.codec.ExecutorServiceCancelOnMemberCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractTargetMessageTask;
import uk.org.hecaton.executor.impl.DistributedExecutorService;
import uk.org.hecaton.executor.impl.operations.CancellationOperation;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.SecurityInterceptorConstants;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.ExecutorServicePermission;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.UUID;

public class ExecutorServiceCancelOnAddressMessageTask
        extends AbstractTargetMessageTask<ExecutorServiceCancelOnMemberCodec.RequestParameters> {

    public ExecutorServiceCancelOnAddressMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new CancellationOperation(parameters.uuid, parameters.interrupt);
    }

    @Override
    protected UUID getTargetUuid() {
        return parameters.memberUUID;
    }

    @Override
    protected ExecutorServiceCancelOnMemberCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return ExecutorServiceCancelOnMemberCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return ExecutorServiceCancelOnMemberCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getDistributedObjectName() {
        DistributedExecutorService service = getService(getServiceName());
        return service.getName(parameters.uuid);
    }

    @Override
    public String getServiceName() {
        return DistributedExecutorService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        String name = getDistributedObjectName();
        if (name == null) {
            // The permission constructor expects a non-null name.
            name = ExecutorServicePermission.EMPTY_EXECUTOR_NAME;
        }

        return new ExecutorServicePermission(name, ActionConstants.ACTION_MODIFY);
    }

    @Override
    public String getMethodName() {
        return SecurityInterceptorConstants.CANCEL;
    }

    @Override
    public Object[] getParameters() {
        return null;
    }
}
