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

package uk.org.hecaton.cp.internal.datastructures.countdownlatch.client;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CountDownLatchGetRoundCodec;
import uk.org.hecaton.cp.internal.client.AbstractCPMessageTask;
import uk.org.hecaton.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import uk.org.hecaton.cp.internal.datastructures.countdownlatch.operation.GetCountOp;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.CountDownLatchPermission;

import java.security.Permission;

import static uk.org.hecaton.cp.internal.raft.QueryPolicy.LINEARIZABLE;

/**
 * Client message task for {@link GetCountOp}
 */
public class GetCountMessageTask extends AbstractCPMessageTask<CountDownLatchGetRoundCodec.RequestParameters> {

    public GetCountMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        query(parameters.groupId, new GetCountOp(parameters.name), LINEARIZABLE);
    }

    @Override
    protected CountDownLatchGetRoundCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CountDownLatchGetRoundCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CountDownLatchGetRoundCodec.encodeResponse((Integer) response);
    }

    @Override
    public String getServiceName() {
        return CountDownLatchService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new CountDownLatchPermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "getCount";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
