/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import uk.org.hecaton.client.impl.protocol.codec.MCForceCloseCPSessionCodec;
import uk.org.hecaton.client.impl.protocol.task.AbstractAsyncMessageTask;
import uk.org.hecaton.cp.session.CPSessionManagementService;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.management.ManagementCenterService;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ManagementPermission;

import java.security.Permission;
import java.util.concurrent.CompletableFuture;

public class ForceCloseCPSessionTask extends AbstractAsyncMessageTask<MCForceCloseCPSessionCodec.RequestParameters, Boolean> {

    private static final Permission REQUIRED_PERMISSION = new ManagementPermission("cp.forceCloseCPSession");

    public ForceCloseCPSessionTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected CompletableFuture<Boolean> processInternal() {
        CPSessionManagementService cpSubsystemManagementService =
                nodeEngine.getHecatonInstance().getCPSubsystem().getCPSessionManagementService();
        return cpSubsystemManagementService.forceCloseSession(parameters.groupName, parameters.sessionId).toCompletableFuture();
    }

    @Override
    protected MCForceCloseCPSessionCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCForceCloseCPSessionCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCForceCloseCPSessionCodec.encodeResponse((boolean) response);
    }

    @Override
    public String getServiceName() {
        return ManagementCenterService.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "forceCloseCPSession";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters};
    }

    @Override
    public Permission getRequiredPermission() {
        return REQUIRED_PERMISSION;
    }

    @Override
    public boolean isManagementTask() {
        return true;
    }
}
