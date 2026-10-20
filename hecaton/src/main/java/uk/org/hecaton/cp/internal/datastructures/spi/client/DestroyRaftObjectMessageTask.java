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

package uk.org.hecaton.cp.internal.datastructures.spi.client;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CPGroupDestroyCPObjectCodec;
import uk.org.hecaton.cp.internal.client.AbstractCPMessageTask;
import uk.org.hecaton.cp.internal.datastructures.spi.operation.DestroyRaftObjectOp;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ActionConstants;

import java.security.Permission;

import static uk.org.hecaton.security.permission.ActionConstants.getPermission;

/**
 * Client message task for destroying Raft objects
 */
public class DestroyRaftObjectMessageTask extends AbstractCPMessageTask<CPGroupDestroyCPObjectCodec.RequestParameters> {

    public DestroyRaftObjectMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        invoke(parameters.groupId, new DestroyRaftObjectOp(parameters.serviceName, parameters.objectName));
    }

    @Override
    protected CPGroupDestroyCPObjectCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CPGroupDestroyCPObjectCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPGroupDestroyCPObjectCodec.encodeResponse();
    }

    @Override
    public String getServiceName() {
        return parameters.serviceName;
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.objectName;
    }

    @Override
    public Permission getRequiredPermission() {
        return getPermission(parameters.objectName, parameters.serviceName, ActionConstants.ACTION_DESTROY);
    }

    @Override
    public String getMethodName() {
        return "destroyRaftObject";
    }

    @Override
    public Object[] getParameters() {
        return new Object[] {parameters.serviceName, parameters.objectName};
    }
}

