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

package uk.org.hecaton.cp.internal.raftop.metadata;

import uk.org.hecaton.cp.internal.CPMemberInfo;
import uk.org.hecaton.cp.internal.IndeterminateOperationStateAware;
import uk.org.hecaton.cp.internal.MetadataRaftGroupManager;
import uk.org.hecaton.cp.internal.RaftService;
import uk.org.hecaton.cp.internal.RaftServiceDataSerializerHook;
import uk.org.hecaton.cp.internal.raft.impl.util.PostponedResponse;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.UUID;

/**
 * When a CP member is shutting down gracefully, or a crashed CP member is
 * removed from CP Subsystem via {@link RaftService#removeCPMember(UUID)},
 * this operation is committed to the Metadata Raft group.
 */
public class RemoveCPMemberOp extends MetadataRaftGroupOp implements IndeterminateOperationStateAware,
                                                                     IdentifiedDataSerializable {

    private CPMemberInfo member;

    public RemoveCPMemberOp() {
    }

    public RemoveCPMemberOp(CPMemberInfo member) {
        this.member = member;
    }

    @Override
    public Object run(MetadataRaftGroupManager metadataGroupManager, long commitIndex) {
        if (metadataGroupManager.removeMember(commitIndex, member)) {
            return null;
        }

        return PostponedResponse.INSTANCE;
    }

    public CPMemberInfo getMember() {
        return member;
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public int getFactoryId() {
        return RaftServiceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftServiceDataSerializerHook.REMOVE_CP_MEMBER_OP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(member);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        member = in.readObject();
    }

    @Override
    protected void toString(StringBuilder sb) {
        sb.append(", member=").append(member);
    }
}
