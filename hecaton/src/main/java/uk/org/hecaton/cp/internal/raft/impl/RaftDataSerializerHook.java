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

package uk.org.hecaton.cp.internal.raft.impl;

import uk.org.hecaton.cp.internal.raft.command.DestroyRaftGroupCmd;
import uk.org.hecaton.cp.internal.raft.impl.command.UpdateRaftGroupMembersCmd;
import uk.org.hecaton.cp.internal.raft.impl.dto.AppendFailureResponse;
import uk.org.hecaton.cp.internal.raft.impl.dto.AppendRequest;
import uk.org.hecaton.cp.internal.raft.impl.dto.AppendSuccessResponse;
import uk.org.hecaton.cp.internal.raft.impl.dto.InstallSnapshot;
import uk.org.hecaton.cp.internal.raft.impl.dto.PreVoteRequest;
import uk.org.hecaton.cp.internal.raft.impl.dto.PreVoteResponse;
import uk.org.hecaton.cp.internal.raft.impl.dto.TriggerLeaderElection;
import uk.org.hecaton.cp.internal.raft.impl.dto.VoteRequest;
import uk.org.hecaton.cp.internal.raft.impl.dto.VoteResponse;
import uk.org.hecaton.cp.internal.raft.impl.log.LogEntry;
import uk.org.hecaton.cp.internal.raft.impl.log.SnapshotEntry;
import uk.org.hecaton.internal.serialization.DataSerializerHook;
import uk.org.hecaton.internal.serialization.impl.FactoryIdHelper;
import uk.org.hecaton.nio.serialization.DataSerializableFactory;

@SuppressWarnings("checkstyle:declarationorder")
public final class RaftDataSerializerHook implements DataSerializerHook {

    private static final int RAFT_DS_FACTORY_ID = -1001;
    private static final String RAFT_DS_FACTORY = "hecaton.serialization.ds.raft";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_DS_FACTORY, RAFT_DS_FACTORY_ID);

    public static final int PRE_VOTE_REQUEST = 1;
    public static final int PRE_VOTE_RESPONSE = 2;
    public static final int VOTE_REQUEST = 3;
    public static final int VOTE_RESPONSE = 4;
    public static final int APPEND_REQUEST = 5;
    public static final int APPEND_SUCCESS_RESPONSE = 6;
    public static final int APPEND_FAILURE_RESPONSE = 7;
    public static final int LOG_ENTRY = 8;
    public static final int SNAPSHOT_ENTRY = 9;
    public static final int INSTALL_SNAPSHOT = 10;
    public static final int DESTROY_RAFT_GROUP_COMMAND = 11;
    public static final int UPDATE_RAFT_GROUP_MEMBERS_COMMAND = 12;
    public static final int TRIGGER_LEADER_ELECTION = 13;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case PRE_VOTE_REQUEST:
                    return new PreVoteRequest();
                case PRE_VOTE_RESPONSE:
                    return new PreVoteResponse();
                case VOTE_REQUEST:
                    return new VoteRequest();
                case VOTE_RESPONSE:
                    return new VoteResponse();
                case APPEND_REQUEST:
                    return new AppendRequest();
                case APPEND_SUCCESS_RESPONSE:
                    return new AppendSuccessResponse();
                case APPEND_FAILURE_RESPONSE:
                    return new AppendFailureResponse();
                case LOG_ENTRY:
                    return new LogEntry();
                case SNAPSHOT_ENTRY:
                    return new SnapshotEntry();
                case INSTALL_SNAPSHOT:
                    return new InstallSnapshot();
                case DESTROY_RAFT_GROUP_COMMAND:
                    return new DestroyRaftGroupCmd();
                case UPDATE_RAFT_GROUP_MEMBERS_COMMAND:
                    return new UpdateRaftGroupMembersCmd();
                case TRIGGER_LEADER_ELECTION:
                    return new TriggerLeaderElection();
                default:
                    throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
