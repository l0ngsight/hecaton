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

package uk.org.hecaton.client.impl.protocol.task.management;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.MCReloadConfigCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractInvocationMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.dynamicconfig.ConfigurationService;
import uk.org.hecaton.internal.management.operation.ReloadConfigOperation;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ManagementPermission;
import uk.org.hecaton.spi.impl.operationservice.InvocationBuilder;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.UUID;

public class ReloadConfigMessageTask
        extends AbstractInvocationMessageTask<Void> {

    private static final ManagementPermission REQUIRED_PERMISSION = new ManagementPermission("config.reload");

    public ReloadConfigMessageTask(ClientMessage clientMessage, Node node,
                                      Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public Permission getRequiredPermission() {
        return REQUIRED_PERMISSION;
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        return nodeEngine.getOperationService().createInvocationBuilder(getServiceName(),
                op, nodeEngine.getThisAddress());
    }

    @Override
    protected Operation prepareOperation() {
        return new ReloadConfigOperation();
    }

    @Override
    protected Void decodeClientMessage(ClientMessage clientMessage) {
        return null;
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCReloadConfigCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getServiceName() {
        return ConfigurationService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "reloadConfig";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

    @Override
    public boolean isManagementTask() {
        return true;
    }
}
