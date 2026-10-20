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

package uk.org.hecaton.cp.internal.datastructures.lock.proxy;

import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.cp.internal.RaftInvocationManager;
import uk.org.hecaton.cp.internal.RaftOp;
import uk.org.hecaton.cp.internal.RaftService;
import uk.org.hecaton.cp.internal.datastructures.lock.LockOwnershipState;
import uk.org.hecaton.cp.internal.datastructures.lock.operation.GetLockOwnershipStateOp;
import uk.org.hecaton.cp.internal.datastructures.lock.operation.LockOp;
import uk.org.hecaton.cp.internal.datastructures.lock.operation.TryLockOp;
import uk.org.hecaton.cp.internal.datastructures.lock.operation.UnlockOp;
import uk.org.hecaton.cp.internal.datastructures.spi.operation.DestroyRaftObjectOp;
import uk.org.hecaton.cp.internal.session.ProxySessionManagerService;
import uk.org.hecaton.cp.lock.FencedLock;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.NodeEngine;

import java.util.UUID;

/**
 * Server-side proxy of Raft-based {@link FencedLock} API
 */
public class FencedLockProxy extends AbstractFencedLockProxy {

    private final RaftInvocationManager invocationManager;

    public FencedLockProxy(NodeEngine nodeEngine, RaftGroupId groupId, String proxyName, String objectName) {
        super(nodeEngine.getService(ProxySessionManagerService.SERVICE_NAME), groupId, proxyName, objectName);
        RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
        this.invocationManager = service.getInvocationManager();
    }

    @Override
    protected final InternalCompletableFuture<Long> doLock(long sessionId, long threadId, UUID invocationUid) {
        return invoke(new LockOp(objectName, sessionId, threadId, invocationUid));
    }

    @Override
    protected final InternalCompletableFuture<Long> doTryLock(long sessionId, long threadId, UUID invocationUid,
                                                              long timeoutMillis) {
        return invoke(new TryLockOp(objectName, sessionId, threadId, invocationUid, timeoutMillis));
    }

    @Override
    protected final InternalCompletableFuture<Boolean> doUnlock(long sessionId, long threadId, UUID invocationUid) {
        return invoke(new UnlockOp(objectName, sessionId, threadId, invocationUid));
    }

    @Override
    protected final InternalCompletableFuture<LockOwnershipState> doGetLockOwnershipState() {
        return invoke(new GetLockOwnershipStateOp(objectName));
    }

    private <T> InternalCompletableFuture<T> invoke(RaftOp op) {
        return invocationManager.invoke(groupId, op);
    }

    @Override
    public void destroy() {
        try {
            invocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), objectName)).joinInternal();
        } finally {
            super.destroy();
        }
    }

}
