/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.internal.cluster.impl.operations;

import uk.org.hecaton.internal.cluster.impl.ClusterHeartbeatManager;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.internal.cluster.impl.MembersViewMetadata;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;

import java.io.IOException;

import static uk.org.hecaton.internal.cluster.impl.ClusterDataSerializerHook.HEARTBEAT_COMPLAINT;

public class HeartbeatComplaintOp extends AbstractClusterOperation {

    private MembersViewMetadata receiverMembersViewMetadata;

    private MembersViewMetadata senderMembersViewMetadata;

    public HeartbeatComplaintOp() {
    }

    public HeartbeatComplaintOp(MembersViewMetadata receiverMembersViewMetadata, MembersViewMetadata senderMembersViewMetadata) {
        this.receiverMembersViewMetadata = receiverMembersViewMetadata;
        this.senderMembersViewMetadata = senderMembersViewMetadata;
    }

    @Override
    public void run() throws Exception {
        ClusterServiceImpl service = getService();
        ClusterHeartbeatManager heartbeatManager = service.getClusterHeartbeatManager();
        heartbeatManager.handleHeartbeatComplaint(receiverMembersViewMetadata, senderMembersViewMetadata);
    }

    @Override
    public int getClassId() {
        return HEARTBEAT_COMPLAINT;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(receiverMembersViewMetadata);
        out.writeObject(senderMembersViewMetadata);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        receiverMembersViewMetadata = in.readObject();
        senderMembersViewMetadata = in.readObject();
    }

}
