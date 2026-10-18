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
import uk.org.hecaton.client.impl.protocol.codec.MCChangeClusterVersionCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCChangeClusterVersionCodec.RequestParameters;
import uk.org.hecaton.client.impl.protocol.task.AbstractInvocationMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.management.ManagementCenterService;
import uk.org.hecaton.internal.management.operation.ChangeClusterVersionOperation;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ManagementPermission;
import uk.org.hecaton.spi.impl.operationservice.InvocationBuilder;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.version.Version;

import java.security.Permission;

public class ChangeClusterVersionMessageTask extends AbstractInvocationMessageTask<RequestParameters> {

    private static final Permission REQUIRED_PERMISSION = new ManagementPermission("cluster.changeVersion");

    public ChangeClusterVersionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {
        return nodeEngine.getOperationService().createInvocationBuilder(getServiceName(),
                op, nodeEngine.getThisAddress());
    }

    @Override
    protected Operation prepareOperation() {
        return new ChangeClusterVersionOperation(
                Version.of(parameters.majorVersion, parameters.minorVersion));
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCChangeClusterVersionCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCChangeClusterVersionCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return ManagementCenterService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return REQUIRED_PERMISSION;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "changeClusterVersion";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.majorVersion, parameters.minorVersion};
    }

    @Override
    public boolean isManagementTask() {
        return true;
    }
}
