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

package uk.org.hecaton.cp.internal.datastructures.semaphore.client;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.SemaphoreAvailablePermitsCodec;
import uk.org.hecaton.cp.internal.client.AbstractCPMessageTask;
import uk.org.hecaton.cp.internal.datastructures.semaphore.SemaphoreService;
import uk.org.hecaton.cp.internal.datastructures.semaphore.operation.AvailablePermitsOp;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.SemaphorePermission;

import java.security.Permission;

import static uk.org.hecaton.cp.internal.raft.QueryPolicy.LINEARIZABLE;

/**
 * Client message task for {@link AvailablePermitsOp}
 */
public class AvailablePermitsMessageTask extends AbstractCPMessageTask<SemaphoreAvailablePermitsCodec.RequestParameters> {

    public AvailablePermitsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        query(parameters.groupId, new AvailablePermitsOp(parameters.name), LINEARIZABLE);
    }

    @Override
    protected SemaphoreAvailablePermitsCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return SemaphoreAvailablePermitsCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return SemaphoreAvailablePermitsCodec.encodeResponse((Integer) response);
    }

    @Override
    public String getServiceName() {
        return SemaphoreService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new SemaphorePermission(parameters.name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "availablePermits";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }
}
