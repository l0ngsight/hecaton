/*
 * Copyright (c) 2026, The Hecaton Authors.
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
package uk.org.hecaton.cp.internal.datastructures.cpmap.client;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CPMapCompareAndSetCodec;
import uk.org.hecaton.cp.internal.RaftOp;
import uk.org.hecaton.cp.internal.client.AbstractCPMessageTask;
import uk.org.hecaton.cp.internal.datastructures.cpmap.CPMapService;
import uk.org.hecaton.cp.internal.datastructures.cpmap.operation.CompareAndSetOp;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.CPMapPermission;

import java.security.Permission;

/**
 * Client message task for {@link uk.org.hecaton.cp.CPMap#compareAndSet}
 */
public class CompareAndSetMessageTask extends AbstractCPMessageTask<CPMapCompareAndSetCodec.RequestParameters> {

    public CompareAndSetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        RaftOp op = new CompareAndSetOp(parameters.name, parameters.key, parameters.expectedValue, parameters.newValue);
        invoke(parameters.groupId, op);
    }

    @Override
    protected CPMapCompareAndSetCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CPMapCompareAndSetCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CPMapCompareAndSetCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return CPMapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new CPMapPermission(parameters.name, ActionConstants.ACTION_PUT);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "compareAndSet";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key, parameters.expectedValue, parameters.newValue};
    }
}
