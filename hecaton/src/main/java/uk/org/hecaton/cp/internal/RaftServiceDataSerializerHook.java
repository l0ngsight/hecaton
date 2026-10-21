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

package uk.org.hecaton.cp.internal;

import uk.org.hecaton.cp.internal.MembershipChangeSchedule.CPGroupMembershipChange;
import uk.org.hecaton.cp.internal.operation.ChangeRaftGroupMembershipOp;
import uk.org.hecaton.cp.internal.operation.DefaultRaftReplicateOp;
import uk.org.hecaton.cp.internal.operation.DestroyRaftGroupOp;
import uk.org.hecaton.cp.internal.operation.GetCPObjectInfosOp;
import uk.org.hecaton.cp.internal.operation.GetLeadedGroupsOp;
import uk.org.hecaton.cp.internal.operation.RaftQueryOp;
import uk.org.hecaton.cp.internal.operation.ResetCPMemberOp;
import uk.org.hecaton.cp.internal.operation.unsafe.UnsafeRaftBackupOp;
import uk.org.hecaton.cp.internal.operation.unsafe.UnsafeRaftQueryOp;
import uk.org.hecaton.cp.internal.operation.unsafe.UnsafeRaftReplicateOp;
import uk.org.hecaton.cp.internal.operation.unsafe.UnsafeSnapshotReplicationOp;
import uk.org.hecaton.cp.internal.operation.TransferLeadershipOp;
import uk.org.hecaton.cp.internal.operation.integration.AppendFailureResponseOp;
import uk.org.hecaton.cp.internal.operation.integration.AppendRequestOp;
import uk.org.hecaton.cp.internal.operation.integration.AppendSuccessResponseOp;
import uk.org.hecaton.cp.internal.operation.integration.InstallSnapshotOp;
import uk.org.hecaton.cp.internal.operation.integration.PreVoteRequestOp;
import uk.org.hecaton.cp.internal.operation.integration.PreVoteResponseOp;
import uk.org.hecaton.cp.internal.operation.integration.TriggerLeaderElectionOp;
import uk.org.hecaton.cp.internal.operation.integration.VoteRequestOp;
import uk.org.hecaton.cp.internal.operation.integration.VoteResponseOp;
import uk.org.hecaton.cp.internal.operation.unsafe.UnsafeStateReplicationOp;
import uk.org.hecaton.cp.internal.raftop.GetInitialRaftGroupMembersIfCurrentGroupMemberOp;
import uk.org.hecaton.cp.internal.raftop.NotifyTermChangeOp;
import uk.org.hecaton.cp.internal.raftop.metadata.AddCPMemberOp;
import uk.org.hecaton.cp.internal.raftop.metadata.CompleteDestroyRaftGroupsOp;
import uk.org.hecaton.cp.internal.raftop.metadata.CompleteRaftGroupMembershipChangesOp;
import uk.org.hecaton.cp.internal.raftop.metadata.CreateRaftGroupOp;
import uk.org.hecaton.cp.internal.raftop.metadata.CreateRaftNodeOp;
import uk.org.hecaton.cp.internal.raftop.metadata.TerminateRaftNodesOp;
import uk.org.hecaton.cp.internal.raftop.metadata.ForceDestroyRaftGroupOp;
import uk.org.hecaton.cp.internal.raftop.metadata.GetActiveCPMembersOp;
import uk.org.hecaton.cp.internal.raftop.metadata.GetActiveRaftGroupByNameOp;
import uk.org.hecaton.cp.internal.raftop.metadata.GetActiveRaftGroupIdsOp;
import uk.org.hecaton.cp.internal.raftop.metadata.GetDestroyingRaftGroupIdsOp;
import uk.org.hecaton.cp.internal.raftop.metadata.GetMembershipChangeScheduleOp;
import uk.org.hecaton.cp.internal.raftop.metadata.GetRaftGroupIdsOp;
import uk.org.hecaton.cp.internal.raftop.metadata.GetRaftGroupOp;
import uk.org.hecaton.cp.internal.raftop.metadata.InitMetadataRaftGroupOp;
import uk.org.hecaton.cp.internal.raftop.metadata.PublishActiveCPMembersOp;
import uk.org.hecaton.cp.internal.raftop.metadata.RaftServicePreJoinOp;
import uk.org.hecaton.cp.internal.raftop.metadata.RemoveCPMemberOp;
import uk.org.hecaton.cp.internal.raftop.metadata.RestartCPMemberOp;
import uk.org.hecaton.cp.internal.raftop.metadata.TriggerDestroyRaftGroupOp;
import uk.org.hecaton.cp.internal.raftop.snapshot.RestoreSnapshotOp;
import uk.org.hecaton.internal.serialization.DataSerializerHook;
import uk.org.hecaton.internal.serialization.impl.FactoryIdHelper;
import uk.org.hecaton.nio.serialization.DataSerializableFactory;

@SuppressWarnings("checkstyle:declarationorder")
public final class RaftServiceDataSerializerHook implements DataSerializerHook {

    private static final int RAFT_DS_FACTORY_ID = -1002;
    private static final String RAFT_DS_FACTORY = "hecaton.serialization.ds.raft.service";

    public static final int F_ID = FactoryIdHelper.getFactoryId(RAFT_DS_FACTORY, RAFT_DS_FACTORY_ID);

    public static final int GROUP_ID = 1;
    public static final int RAFT_GROUP_INFO = 2;
    public static final int PRE_VOTE_REQUEST_OP = 3;
    public static final int PRE_VOTE_RESPONSE_OP = 4;
    public static final int VOTE_REQUEST_OP = 5;
    public static final int VOTE_RESPONSE_OP = 6;
    public static final int APPEND_REQUEST_OP = 7;
    public static final int APPEND_SUCCESS_RESPONSE_OP = 8;
    public static final int APPEND_FAILURE_RESPONSE_OP = 9;
    public static final int METADATA_RAFT_GROUP_SNAPSHOT = 10;
    public static final int INSTALL_SNAPSHOT_OP = 11;
    public static final int DEFAULT_RAFT_GROUP_REPLICATE_OP = 12;
    public static final int CREATE_RAFT_GROUP_OP = 13;
    public static final int TRIGGER_DESTROY_RAFT_GROUP_OP = 14;
    public static final int COMPLETE_DESTROY_RAFT_GROUPS_OP = 15;
    public static final int REMOVE_CP_MEMBER_OP = 16;
    public static final int COMPLETE_RAFT_GROUP_MEMBERSHIP_CHANGES_OP = 17;
    public static final int MEMBERSHIP_CHANGE_REPLICATE_OP = 18;
    public static final int MEMBERSHIP_CHANGE_SCHEDULE = 19;
    public static final int DEFAULT_RAFT_GROUP_QUERY_OP = 20;
    public static final int TERMINATE_RAFT_NODES_OP = 21;
    public static final int GET_ACTIVE_CP_MEMBERS_OP = 22;
    public static final int GET_DESTROYING_RAFT_GROUP_IDS_OP = 23;
    public static final int GET_MEMBERSHIP_CHANGE_SCHEDULE_OP = 24;
    public static final int GET_RAFT_GROUP_OP = 25;
    public static final int GET_ACTIVE_RAFT_GROUP_BY_NAME_OP = 26;
    public static final int CREATE_RAFT_NODE_OP = 27;
    public static final int DESTROY_RAFT_GROUP_OP = 28;
    public static final int RESTORE_SNAPSHOT_OP = 29;
    public static final int NOTIFY_TERM_CHANGE_OP = 30;
    public static final int CP_MEMBER = 31;
    public static final int PUBLISH_ACTIVE_CP_MEMBERS_OP = 32;
    public static final int ADD_CP_MEMBER_OP = 33;
    public static final int INIT_METADATA_RAFT_GROUP_OP = 34;
    public static final int FORCE_DESTROY_RAFT_GROUP_OP = 35;
    public static final int GET_INITIAL_RAFT_GROUP_MEMBERS_IF_CURRENT_GROUP_MEMBER_OP = 36;
    public static final int GET_RAFT_GROUP_IDS_OP = 37;
    public static final int GET_ACTIVE_RAFT_GROUP_IDS_OP = 38;
    public static final int RAFT_PRE_JOIN_OP = 39;
    public static final int RESET_CP_MEMBER_OP = 40;
    public static final int GROUP_MEMBERSHIP_CHANGE = 41;
    public static final int UNSAFE_RAFT_REPLICATE_OP = 42;
    public static final int UNSAFE_RAFT_QUERY_OP = 43;
    public static final int UNSAFE_RAFT_BACKUP_OP = 44;
    public static final int UNSAFE_SNAPSHOT_REPLICATE_OP = 45;
    public static final int CP_ENDPOINT = 46;
    public static final int CP_GROUP_SUMMARY = 47;
    public static final int GET_LEADED_GROUPS = 48;
    public static final int TRANSFER_LEADERSHIP_OP = 49;
    public static final int TRIGGER_LEADER_ELECTION_OP = 50;
    public static final int UNSAFE_MODE_PARTITION_STATE = 51;
    public static final int UNSAFE_STATE_REPLICATE_OP = 52;
    public static final int GET_CP_OBJECT_INFOS_OP = 53;
    public static final int RESTART_CP_MEMBER_OP = 54;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> {
            switch (typeId) {
                case GROUP_ID:
                    return new RaftGroupId();
                case RAFT_GROUP_INFO:
                    return new CPGroupInfo();
                case PRE_VOTE_REQUEST_OP:
                    return new PreVoteRequestOp();
                case PRE_VOTE_RESPONSE_OP:
                    return new PreVoteResponseOp();
                case VOTE_REQUEST_OP:
                    return new VoteRequestOp();
                case VOTE_RESPONSE_OP:
                    return new VoteResponseOp();
                case APPEND_REQUEST_OP:
                    return new AppendRequestOp();
                case APPEND_SUCCESS_RESPONSE_OP:
                    return new AppendSuccessResponseOp();
                case APPEND_FAILURE_RESPONSE_OP:
                    return new AppendFailureResponseOp();
                case METADATA_RAFT_GROUP_SNAPSHOT:
                    return new MetadataRaftGroupSnapshot();
                case INSTALL_SNAPSHOT_OP:
                    return new InstallSnapshotOp();
                case CREATE_RAFT_GROUP_OP:
                    return new CreateRaftGroupOp();
                case DEFAULT_RAFT_GROUP_REPLICATE_OP:
                    return new DefaultRaftReplicateOp();
                case TRIGGER_DESTROY_RAFT_GROUP_OP:
                    return new TriggerDestroyRaftGroupOp();
                case COMPLETE_DESTROY_RAFT_GROUPS_OP:
                    return new CompleteDestroyRaftGroupsOp();
                case REMOVE_CP_MEMBER_OP:
                    return new RemoveCPMemberOp();
                case COMPLETE_RAFT_GROUP_MEMBERSHIP_CHANGES_OP:
                    return new CompleteRaftGroupMembershipChangesOp();
                case MEMBERSHIP_CHANGE_REPLICATE_OP:
                    return new ChangeRaftGroupMembershipOp();
                case MEMBERSHIP_CHANGE_SCHEDULE:
                    return new MembershipChangeSchedule();
                case DEFAULT_RAFT_GROUP_QUERY_OP:
                    return new RaftQueryOp();
                case TERMINATE_RAFT_NODES_OP:
                    return new TerminateRaftNodesOp();
                case GET_ACTIVE_CP_MEMBERS_OP:
                    return new GetActiveCPMembersOp();
                case GET_DESTROYING_RAFT_GROUP_IDS_OP:
                    return new GetDestroyingRaftGroupIdsOp();
                case GET_MEMBERSHIP_CHANGE_SCHEDULE_OP:
                    return new GetMembershipChangeScheduleOp();
                case GET_RAFT_GROUP_OP:
                    return new GetRaftGroupOp();
                case GET_ACTIVE_RAFT_GROUP_BY_NAME_OP:
                    return new GetActiveRaftGroupByNameOp();
                case CREATE_RAFT_NODE_OP:
                    return new CreateRaftNodeOp();
                case DESTROY_RAFT_GROUP_OP:
                    return new DestroyRaftGroupOp();
                case RESTORE_SNAPSHOT_OP:
                    return new RestoreSnapshotOp();
                case NOTIFY_TERM_CHANGE_OP:
                    return new NotifyTermChangeOp();
                case CP_MEMBER:
                    return new CPMemberInfo();
                case PUBLISH_ACTIVE_CP_MEMBERS_OP:
                    return new PublishActiveCPMembersOp();
                case ADD_CP_MEMBER_OP:
                    return new AddCPMemberOp();
                case INIT_METADATA_RAFT_GROUP_OP:
                    return new InitMetadataRaftGroupOp();
                case FORCE_DESTROY_RAFT_GROUP_OP:
                    return new ForceDestroyRaftGroupOp();
                case GET_INITIAL_RAFT_GROUP_MEMBERS_IF_CURRENT_GROUP_MEMBER_OP:
                    return new GetInitialRaftGroupMembersIfCurrentGroupMemberOp();
                case GET_RAFT_GROUP_IDS_OP:
                    return new GetRaftGroupIdsOp();
                case GET_ACTIVE_RAFT_GROUP_IDS_OP:
                    return new GetActiveRaftGroupIdsOp();
                case RAFT_PRE_JOIN_OP:
                    return new RaftServicePreJoinOp();
                case RESET_CP_MEMBER_OP:
                    return new ResetCPMemberOp();
                case GROUP_MEMBERSHIP_CHANGE:
                    return new CPGroupMembershipChange();
                case UNSAFE_RAFT_REPLICATE_OP:
                    return new UnsafeRaftReplicateOp();
                case UNSAFE_RAFT_QUERY_OP:
                    return new UnsafeRaftQueryOp();
                case UNSAFE_RAFT_BACKUP_OP:
                    return new UnsafeRaftBackupOp();
                case UNSAFE_SNAPSHOT_REPLICATE_OP:
                    return new UnsafeSnapshotReplicationOp();
                case CP_ENDPOINT:
                    return new RaftEndpointImpl();
                case CP_GROUP_SUMMARY:
                    return new CPGroupSummary();
                case GET_LEADED_GROUPS:
                    return new GetLeadedGroupsOp();
                case TRANSFER_LEADERSHIP_OP:
                    return new TransferLeadershipOp();
                case TRIGGER_LEADER_ELECTION_OP:
                    return new TriggerLeaderElectionOp();
                case UNSAFE_MODE_PARTITION_STATE:
                    return new UnsafeModePartitionState();
                case UNSAFE_STATE_REPLICATE_OP:
                    return new UnsafeStateReplicationOp();
                case GET_CP_OBJECT_INFOS_OP:
                    return new GetCPObjectInfosOp();
                case RESTART_CP_MEMBER_OP:
                    return new RestartCPMemberOp();
                default:
                    throw new IllegalArgumentException("Undefined type: " + typeId);
            }
        };
    }
}
