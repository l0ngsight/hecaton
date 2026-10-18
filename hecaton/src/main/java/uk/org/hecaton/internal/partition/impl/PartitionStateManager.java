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
package uk.org.hecaton.internal.partition.impl;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.instance.impl.NodeExtension;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.partition.PartitionReplica;
import uk.org.hecaton.internal.partition.PartitionReplicaInterceptor;
import uk.org.hecaton.internal.partition.PartitionTableView;
import uk.org.hecaton.internal.util.collection.PartitionIdSet;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;

public interface PartitionStateManager {

    /**
     * Initial value of the partition table stamp.
     * If stamp has this initial value then that means
     * partition table is not initialized yet.
     */
    long INITIAL_STAMP = 0L;

    /**
     * @return {@code true} if there are partitions having {@link
     * InternalPartitionImpl#isMigrating()} flag set, {@code false} otherwise.
     */
    boolean hasMigratingPartitions();

    /**
     * Arranges the partitions if:
     * <ul>
     * <li>this instance {@link NodeExtension#isStartCompleted()}</li>
     * <li>the cluster state allows migrations. See {@link ClusterState#isMigrationAllowed()}</li>
     * </ul>
     * This will also set the manager state to initialized (if not already) and invoke the
     * {@link DefaultPartitionReplicaInterceptor} for all changed replicas which
     * will cancel replica synchronizations and increase the partition state version.
     *
     * @param excludedMembers members which are to be excluded from the new layout
     * @return if the new partition was assigned
     * @throws HecatonException if the partition state generator failed to arrange the partitions
     */
    boolean initializePartitionAssignments(Set<Member> excludedMembers);

    /**
     * Called after a batch of partition replica assignments have been applied. This is an optimization for batch
     * changes, to avoid repeatedly performing costly computations (like updating partition assignments stamp).
     * <p><b>
     * If this logic changes, consider also changing the implementation of
     * {@link PartitionReplicaInterceptor#replicaChanged(int, int, PartitionReplica, PartitionReplica)}, which should apply
     * the same logic per partition.
     * </b></p>
     */
    void partitionOwnersChanged(PartitionIdSet partitionIdSet);

    /**
     * Sets the initial partition table and state version. If any partition has a replica, the partition state manager is
     * set to initialized, otherwise {@link #isInitialized()} stays uninitialized but the current state will be updated
     * nevertheless.
     *
     * @param partitionTable the initial partition table
     * @throws IllegalStateException if the partition manager has already been initialized
     */
    void setInitialState(PartitionTableView partitionTable);

    void updateMemberGroupsSize();

    int getMemberGroupsSize();

    /**
     * Checks all replicas for all partitions. If the cluster service does not contain the member or the member
     * is a lite member for any address in the partition table, it will remove the address from the partition.
     *
     * @see ClusterService#getMember(Address, UUID)
     */
    void removeUnknownAndLiteMembers();

    boolean isAbsentInPartitionTable(Member member);

    InternalPartition[] getPartitions();


    /**
     * Returns a copy of the current partition table.
     */
    InternalPartition[] getPartitionsCopy(boolean readonly);

    InternalPartitionImpl getPartitionImpl(int partitionId);

    PartitionReplica[][] repartition(Set<Member> excludedMembers, Collection<Integer> partitionInclusionSet);

    boolean trySetMigratingFlag(int partitionId);

    void clearMigratingFlag(int partitionId);

    boolean isMigrating(int partitionId);

    void updateStamp();

    long getStamp();

    int getPartitionVersion(int partitionId);

    /**
     * Increments partition version by delta and updates partition state stamp.
     */
    void incrementPartitionVersion(int partitionId, int delta);

    boolean setInitialized();

    boolean isInitialized();

    void reset();

    int replaceMember(Member oldMember, Member newMember);

    PartitionTableView getPartitionTable();

    void storeSnapshot(UUID crashedMemberUuid);

    Collection<PartitionTableView> snapshots();

    PartitionTableView getSnapshot(UUID crashedMemberUuid);

    void removeSnapshot(UUID memberUuid);

    /** For test usage only */
    void setReplicaUpdateInterceptor(ReplicaUpdateInterceptor interceptor);

    interface ReplicaUpdateInterceptor {
        void onPartitionOwnersChanged();
        void onPartitionStampUpdate();
    }
}
