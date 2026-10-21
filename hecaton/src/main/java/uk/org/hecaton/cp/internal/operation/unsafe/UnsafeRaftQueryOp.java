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

package uk.org.hecaton.cp.internal.operation.unsafe;

import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.internal.RaftOp;
import uk.org.hecaton.cp.internal.RaftServiceDataSerializerHook;
import uk.org.hecaton.spi.impl.operationservice.CallStatus;
import uk.org.hecaton.spi.impl.operationservice.ReadonlyOperation;

/**
 * Partition-based Raft query operation for Raft services to be executed in UNSAFE mode.
 * This operation wraps original {@link RaftOp} and executes it as a query operation
 * outside of Raft algorithm.
 */
public class UnsafeRaftQueryOp extends AbstractUnsafeRaftOp implements ReadonlyOperation {

    public UnsafeRaftQueryOp() {
    }

    public UnsafeRaftQueryOp(CPGroupId groupId, RaftOp op) {
        super(groupId, op);
    }

    @Override
    CallStatus handleResponse(long commitIndex, Object response) {
        return CallStatus.RESPONSE;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.UNSAFE_RAFT_QUERY_OP;
    }
}
