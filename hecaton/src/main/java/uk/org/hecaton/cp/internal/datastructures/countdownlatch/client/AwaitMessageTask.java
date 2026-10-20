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
import uk.org.hecaton.client.impl.protocol.codec.CountDownLatchAwaitCodec;
import uk.org.hecaton.cp.internal.client.AbstractCPMessageTask;
import uk.org.hecaton.cp.internal.datastructures.countdownlatch.CountDownLatchService;
import uk.org.hecaton.cp.internal.datastructures.countdownlatch.operation.AwaitOp;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.CountDownLatchPermission;

import java.security.Permission;
import java.util.concurrent.TimeUnit;

/**
 * Client message task for {@link AwaitOp}
 */
public class AwaitMessageTask extends AbstractCPMessageTask<CountDownLatchAwaitCodec.RequestParameters> {

    public AwaitMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        invoke(parameters.groupId, new AwaitOp(parameters.name, parameters.invocationUid, parameters.timeoutMs));
    }

    @Override
    protected CountDownLatchAwaitCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return CountDownLatchAwaitCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return CountDownLatchAwaitCodec.encodeResponse((Boolean) response);
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
        return "await";
    }

    @Override
    public Object[] getParameters() {
        return new Object[] {parameters.timeoutMs, TimeUnit.MILLISECONDS};
    }
}
