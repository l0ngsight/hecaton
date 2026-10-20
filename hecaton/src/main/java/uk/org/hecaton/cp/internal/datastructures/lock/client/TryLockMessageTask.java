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

package uk.org.hecaton.cp.internal.datastructures.lock.client;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.FencedLockTryLockCodec;
import uk.org.hecaton.cp.internal.RaftOp;
import uk.org.hecaton.cp.internal.client.AbstractCPMessageTask;
import uk.org.hecaton.cp.internal.datastructures.lock.LockService;
import uk.org.hecaton.cp.internal.datastructures.lock.operation.TryLockOp;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.security.permission.ActionConstants;
import uk.org.hecaton.security.permission.LockPermission;

import java.security.Permission;
import java.util.concurrent.TimeUnit;

/**
 * Client message task for {@link TryLockOp}
 */
public class TryLockMessageTask extends AbstractCPMessageTask<FencedLockTryLockCodec.RequestParameters> {

    public TryLockMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected void processMessage() {
        RaftOp op = new TryLockOp(parameters.name, parameters.sessionId, parameters.threadId, parameters.invocationUid,
                parameters.timeoutMs);
        invoke(parameters.groupId, op);
    }

    @Override
    protected FencedLockTryLockCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return FencedLockTryLockCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return FencedLockTryLockCodec.encodeResponse((Long) response);
    }

    @Override
    public String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new LockPermission(parameters.name, ActionConstants.ACTION_LOCK);
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "tryLock";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.timeoutMs, TimeUnit.MILLISECONDS};
    }
}
