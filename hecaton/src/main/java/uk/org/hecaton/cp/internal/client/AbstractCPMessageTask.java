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

package uk.org.hecaton.cp.internal.client;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.task.AbstractMessageTask;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.internal.RaftInvocationManager;
import uk.org.hecaton.cp.internal.RaftOp;
import uk.org.hecaton.cp.internal.RaftService;
import uk.org.hecaton.cp.internal.raft.QueryPolicy;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;

import java.util.function.BiConsumer;

import static uk.org.hecaton.internal.util.ConcurrencyUtil.CALLER_RUNS;

public abstract class AbstractCPMessageTask<P> extends AbstractMessageTask<P> implements BiConsumer<Object, Throwable> {

    protected AbstractCPMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    protected void query(CPGroupId groupId, RaftOp op, QueryPolicy policy) {
        RaftInvocationManager invocationManager = getInvocationManager();
        InternalCompletableFuture<Object> future = invocationManager.query(groupId, op, policy, false);
        future.whenCompleteAsync(this, CALLER_RUNS);
    }

    protected void invoke(CPGroupId groupId, RaftOp op) {
        RaftInvocationManager invocationManager = getInvocationManager();
        InternalCompletableFuture<Object> future = invocationManager.invoke(groupId, op, false);
        future.whenCompleteAsync(this, CALLER_RUNS);
    }

    private RaftInvocationManager getInvocationManager() {
        RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
        return service.getInvocationManager();
    }

    @Override
    public void accept(Object response, Throwable throwable) {
        if (throwable == null) {
            sendResponse(response);
        } else {
            handleProcessingFailure(throwable);
        }
    }
}
