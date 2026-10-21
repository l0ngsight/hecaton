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

package uk.org.hecaton.cp.internal.operation.integration;

import uk.org.hecaton.cp.internal.raft.impl.RaftEndpoint;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.internal.RaftService;
import uk.org.hecaton.cp.internal.RaftServiceDataSerializerHook;
import uk.org.hecaton.cp.internal.RaftSystemOperation;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.io.IOException;

/**
 * Base class for operation classes that will carry Raft RPCs between
 * Raft nodes
 */
public abstract class AsyncRaftOp extends Operation implements IdentifiedDataSerializable, RaftSystemOperation {

    protected CPGroupId groupId;
    protected RaftEndpoint target;

    AsyncRaftOp() {
    }

    AsyncRaftOp(CPGroupId groupId) {
        this.groupId = groupId;
    }

    public final Operation setTargetEndpoint(RaftEndpoint target) {
        this.target = target;
        return this;
    }

    @Override
    public final boolean returnsResponse() {
        return false;
    }

    @Override
    public final Object getResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    // Raft ops are executed on partition threads but not sent to partition owners.
    @Override
    public final boolean validatesTarget() {
        return false;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(groupId);
        out.writeObject(target);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        groupId = in.readObject();
        target = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }
}
