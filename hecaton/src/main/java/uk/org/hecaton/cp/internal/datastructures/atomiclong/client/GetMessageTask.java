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

package uk.org.hecaton.cp.internal.datastructures.atomiclong.client;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.AtomicLongGetCodec;
import uk.org.hecaton.cp.internal.client.AbstractCPMessageTask;
import uk.org.hecaton.cp.internal.datastructures.atomiclong.AtomicLongService;
import uk.org.hecaton.cp.internal.datastructures.atomiclong.operation.GetAndAddOp;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.AtomicLongPermission;

import java.security.Permission;

import static uk.org.hecaton.cp.internal.raft.QueryPolicy.LINEARIZABLE;

/**
 * Client message task for {@link GetAndAddOp}
 */
public class GetMessageTask extends AbstractCPMessageTask<AtomicLongGetCodec.RequestParameters> {

    public GetMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
       query(parameters.groupId, new GetAndAddOp(parameters.name, 0), LINEARIZABLE);
    }

    @Override
    protected AtomicLongGetCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return AtomicLongGetCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return AtomicLongGetCodec.encodeResponse((Long) response);
    }

    @Override
    public String getServiceName() {
        return AtomicLongService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new AtomicLongPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "get";
    }

    public Object[] getParameters() {
        return new Object[0];
    }
}
