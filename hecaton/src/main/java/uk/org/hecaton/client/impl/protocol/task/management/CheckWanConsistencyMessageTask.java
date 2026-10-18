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
import uk.org.hecaton.client.impl.protocol.codec.MCCheckWanConsistencyCodec;
import uk.org.hecaton.client.impl.protocol.codec.MCCheckWanConsistencyCodec.RequestParameters;
import uk.org.hecaton.client.impl.protocol.task.AbstractCallableMessageTask;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ManagementPermission;
import uk.org.hecaton.wan.impl.WanReplicationService;

import java.security.Permission;
import java.util.UUID;

public class CheckWanConsistencyMessageTask extends AbstractCallableMessageTask<RequestParameters> {

    private static final Permission REQUIRED_PERMISSION = new ManagementPermission("wan.checkConsistency");

    public CheckWanConsistencyMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Object call() throws Exception {
        return nodeEngine.getWanReplicationService().consistencyCheck(
                parameters.wanReplicationName, parameters.wanPublisherId, parameters.mapName);
    }

    @Override
    protected RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MCCheckWanConsistencyCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MCCheckWanConsistencyCodec.encodeResponse((UUID) response);
    }

    @Override
    public String getServiceName() {
        return WanReplicationService.SERVICE_NAME;
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
        return "checkWanConsistency";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.wanReplicationName, parameters.wanPublisherId, parameters.mapName};
    }

    @Override
    public boolean isManagementTask() {
        return true;
    }
}
