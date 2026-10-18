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

import uk.org.hecaton.internal.partition.MigrationStateImpl;
import uk.org.hecaton.internal.partition.NonFragmentedServiceNamespace;
import uk.org.hecaton.internal.partition.PartitionLostEventImpl;
import uk.org.hecaton.internal.partition.PartitionReplica;
import uk.org.hecaton.internal.partition.PartitionRuntimeState;
import uk.org.hecaton.internal.partition.ReplicaFragmentMigrationState;
import uk.org.hecaton.internal.partition.ReplicaMigrationEventImpl;
import uk.org.hecaton.internal.partition.operation.AssignPartitions;
import uk.org.hecaton.internal.partition.operation.DemoteRequestOperation;
import uk.org.hecaton.internal.partition.operation.DemoteResponseOperation;
import uk.org.hecaton.internal.partition.operation.FetchPartitionStateOperation;
import uk.org.hecaton.internal.partition.operation.HasOngoingMigration;
import uk.org.hecaton.internal.partition.operation.MigrationCommitOperation;
import uk.org.hecaton.internal.partition.operation.MigrationOperation;
import uk.org.hecaton.internal.partition.operation.MigrationRequestOperation;
import uk.org.hecaton.internal.partition.operation.PartitionBackupReplicaAntiEntropyOperation;
import uk.org.hecaton.internal.partition.operation.PartitionReplicaSyncRequest;
import uk.org.hecaton.internal.partition.operation.PartitionReplicaSyncRequestOffloadable;
import uk.org.hecaton.internal.partition.operation.PartitionReplicaSyncResponse;
import uk.org.hecaton.internal.partition.operation.PartitionReplicaSyncRetryResponse;
import uk.org.hecaton.internal.partition.operation.PartitionStateCheckOperation;
import uk.org.hecaton.internal.partition.operation.PartitionStateOperation;
import uk.org.hecaton.internal.partition.operation.PromotionCommitOperation;
import uk.org.hecaton.internal.partition.operation.PublishCompletedMigrationsOperation;
import uk.org.hecaton.internal.partition.operation.SafeStateCheckOperation;
import uk.org.hecaton.internal.partition.operation.ShutdownRequestOperation;
import uk.org.hecaton.internal.partition.operation.ShutdownResponseOperation;
import uk.org.hecaton.internal.serialization.DataSerializerHook;
import uk.org.hecaton.internal.serialization.impl.ArrayDataSerializableFactory;
import uk.org.hecaton.internal.serialization.impl.FactoryIdHelper;
import uk.org.hecaton.nio.serialization.DataSerializableFactory;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;

import java.util.function.Supplier;

import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.PARTITION_DS_FACTORY;
import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.PARTITION_DS_FACTORY_ID;

@SuppressWarnings("ClassDataAbstractionCoupling")
public final class PartitionDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(PARTITION_DS_FACTORY, PARTITION_DS_FACTORY_ID);

    public static final int PARTITION_RUNTIME_STATE = 1;
    public static final int ASSIGN_PARTITIONS = 2;
    public static final int PARTITION_BACKUP_REPLICA_ANTI_ENTROPY = 3;
    public static final int FETCH_PARTITION_STATE = 4;
    public static final int HAS_ONGOING_MIGRATION = 5;
    public static final int MIGRATION_COMMIT = 6;
    public static final int PARTITION_STATE_OP = 7;
    public static final int PROMOTION_COMMIT = 8;
    public static final int REPLICA_SYNC_REQUEST = 9;
    public static final int REPLICA_SYNC_RESPONSE = 10;
    public static final int REPLICA_SYNC_RETRY_RESPONSE = 11;
    public static final int SAFE_STATE_CHECK = 12;
    public static final int SHUTDOWN_REQUEST = 13;
    public static final int SHUTDOWN_RESPONSE = 14;
    public static final int REPLICA_FRAGMENT_MIGRATION_STATE = 15;
    public static final int MIGRATION = 16;
    public static final int MIGRATION_REQUEST = 17;
    public static final int NON_FRAGMENTED_SERVICE_NAMESPACE = 18;
    public static final int PARTITION_REPLICA = 19;
    public static final int PUBLISH_COMPLETED_MIGRATIONS = 20;
    public static final int PARTITION_STATE_CHECK_OP = 21;
    public static final int REPLICA_MIGRATION_EVENT = 22;
    public static final int MIGRATION_EVENT = 23;
    public static final int PARTITION_LOST_EVENT = 24;
    public static final int REPLICA_SYNC_REQUEST_OFFLOADABLE = 25;
    public static final int DEMOTE_REQUEST = 26;
    public static final int DEMOTE_RESPONSE = 27;

    private static final int LEN = DEMOTE_RESPONSE + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        Supplier<IdentifiedDataSerializable>[] constructors = new Supplier[LEN];

        constructors[PARTITION_RUNTIME_STATE] = PartitionRuntimeState::new;
        constructors[ASSIGN_PARTITIONS] = AssignPartitions::new;
        constructors[PARTITION_BACKUP_REPLICA_ANTI_ENTROPY] = PartitionBackupReplicaAntiEntropyOperation::new;
        constructors[FETCH_PARTITION_STATE] = FetchPartitionStateOperation::new;
        constructors[HAS_ONGOING_MIGRATION] = HasOngoingMigration::new;
        constructors[MIGRATION_COMMIT] = MigrationCommitOperation::new;
        constructors[PARTITION_STATE_OP] = PartitionStateOperation::new;
        constructors[PROMOTION_COMMIT] = PromotionCommitOperation::new;
        constructors[REPLICA_SYNC_REQUEST] = PartitionReplicaSyncRequest::new;
        constructors[REPLICA_SYNC_RESPONSE] = PartitionReplicaSyncResponse::new;
        constructors[REPLICA_SYNC_RETRY_RESPONSE] = PartitionReplicaSyncRetryResponse::new;
        constructors[SAFE_STATE_CHECK] = SafeStateCheckOperation::new;
        constructors[SHUTDOWN_REQUEST] = ShutdownRequestOperation::new;
        constructors[SHUTDOWN_RESPONSE] = ShutdownResponseOperation::new;
        constructors[REPLICA_FRAGMENT_MIGRATION_STATE] = ReplicaFragmentMigrationState::new;
        constructors[MIGRATION] = MigrationOperation::new;
        constructors[MIGRATION_REQUEST] = MigrationRequestOperation::new;
        constructors[NON_FRAGMENTED_SERVICE_NAMESPACE] = () -> NonFragmentedServiceNamespace.INSTANCE;
        constructors[PARTITION_REPLICA] = PartitionReplica::new;
        constructors[PUBLISH_COMPLETED_MIGRATIONS] = PublishCompletedMigrationsOperation::new;
        constructors[PARTITION_STATE_CHECK_OP] = PartitionStateCheckOperation::new;
        constructors[REPLICA_MIGRATION_EVENT] = ReplicaMigrationEventImpl::new;
        constructors[MIGRATION_EVENT] = MigrationStateImpl::new;
        constructors[PARTITION_LOST_EVENT] = PartitionLostEventImpl::new;
        constructors[REPLICA_SYNC_REQUEST_OFFLOADABLE] = PartitionReplicaSyncRequestOffloadable::new;
        constructors[DEMOTE_REQUEST] = DemoteRequestOperation::new;
        constructors[DEMOTE_RESPONSE] = DemoteResponseOperation::new;

        return new ArrayDataSerializableFactory(constructors);
    }
}
