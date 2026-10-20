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

package uk.org.hecaton.cp.internal.datastructures.atomicref.client;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.AtomicRefCompareAndSetCodec;
import uk.org.hecaton.cp.internal.RaftOp;
import uk.org.hecaton.cp.internal.client.AbstractCPMessageTask;
import uk.org.hecaton.cp.internal.datastructures.atomicref.AtomicRefService;
import uk.org.hecaton.cp.internal.datastructures.atomicref.operation.CompareAndSetOp;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.AtomicReferencePermission;

import java.security.Permission;

/**
 * Client message task for {@link CompareAndSetOp}
 */
public class CompareAndSetMessageTask extends AbstractCPMessageTask<AtomicRefCompareAndSetCodec.RequestParameters> {

    public CompareAndSetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        RaftOp op = new CompareAndSetOp(parameters.name, parameters.oldValue, parameters.newValue);
        invoke(parameters.groupId, op);
    }

    @Override
    protected AtomicRefCompareAndSetCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return AtomicRefCompareAndSetCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return AtomicRefCompareAndSetCodec.encodeResponse((Boolean) response);
    }

    @Override
    public String getServiceName() {
        return AtomicRefService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new AtomicReferencePermission(parameters.name, ActionConstants.ACTION_MODIFY);
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
        return new Object[]{parameters.oldValue, parameters.newValue};
    }
}
