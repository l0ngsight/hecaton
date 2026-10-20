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

import uk.org.hecaton.cp.internal.RaftService;
import uk.org.hecaton.cp.internal.RaftServiceDataSerializerHook;
import uk.org.hecaton.cp.internal.UnsafeModePartitionState;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.io.IOException;

/**
 * Partition-based replication operation to migrate {@linkplain RaftService}'s state in UNSAFE mode.
 */
public class UnsafeStateReplicationOp extends Operation implements IdentifiedDataSerializable {

    private UnsafeModePartitionState state;

    public UnsafeStateReplicationOp() {
    }

    public UnsafeStateReplicationOp(UnsafeModePartitionState state) {
        this.state = state;
    }

    @Override
    public void run() throws Exception {
        RaftService service = getService();
        service.applyUnsafeModeState(getPartitionId(), state);
    }

    @Override
    public String getServiceName() {
        return RaftService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.UNSAFE_STATE_REPLICATE_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(state);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        state = in.readObject();
    }
}
