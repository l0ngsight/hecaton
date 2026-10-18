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

package uk.org.hecaton.internal.partition.operation;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.internal.partition.MigrationInfo;
import uk.org.hecaton.internal.partition.PartitionReplica;
import uk.org.hecaton.internal.partition.impl.InternalPartitionImpl;
import uk.org.hecaton.spi.exception.RetryableHecatonException;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.operationservice.InvocationBuilder;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.internal.partition.IPartitionService.SERVICE_NAME;
import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.Accessors.getOperationService;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MigrationRequestOperationTest extends HecatonTestSupport {

    private HecatonInstance member1;
    private HecatonInstance member2;
    @Before
    public void init() {
        HecatonInstance[] members = createHecatonInstances(2);
        member1 = members[0];
        member2 = members[1];
        warmUpPartitions(members);
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Test
    public void whenPartitionIsNotMigrating_shouldSetFlag() {
        MigrationInfo migration = createMigrationInfo(member1, member2);
        final int partitionId = migration.getPartitionId();

        // set partition as migrating on owner
        final InternalPartitionImpl partition = getInternalPartition(member2, partitionId);

        // request migration
        Operation op = new MigrationRequestOperation(migration, Collections.emptyList(),
                0, true, true, Integer.MAX_VALUE);

        InvocationBuilder invocationBuilder = getOperationService(member1)
                .createInvocationBuilder(SERVICE_NAME, op, getAddress(member2))
                .setCallTimeout(TimeUnit.MINUTES.toMillis(1))
                .setAsync();

        assertCompletesEventually(invocationBuilder.invoke());

        // flag would be cleared later by finalizeMigration
        assertThat(partition.isMigrating()).as("Should not clear migrating flag after operation").isTrue();
    }

    // in practice migrating flag may be set by PartitionReplicaSyncRequestOffloadable
    // or possibly previous, still running migration.
    @Test
    public void whenPartitionIsMigrating_shouldRetryAndNotClearFlag() {
        MigrationInfo migration = createMigrationInfo(member1, member2);
        final int partitionId = migration.getPartitionId();

        // set partition as migrating on owner
        final InternalPartitionImpl partition = getInternalPartition(member2, partitionId);
        partition.setMigrating();

        // request migration
        Operation op = new MigrationRequestOperation(migration, Collections.emptyList(),
                0, true, true, Integer.MAX_VALUE);

        InvocationBuilder invocationBuilder = getOperationService(member1)
                .createInvocationBuilder(SERVICE_NAME, op, getAddress(member2))
                .setTryCount(2)
                .setCallTimeout(TimeUnit.MINUTES.toMillis(1));

        assertThat(invocationBuilder.invoke()).failsWithin(ASSERT_TRUE_EVENTUALLY_TIMEOUT, TimeUnit.SECONDS)
                .withThrowableThat().havingRootCause()
                .isInstanceOf(RetryableHecatonException.class)
                .withMessageContaining("Cannot set migrating flag, probably previous migration's finalization is not completed yet.");
        assertThat(partition.isMigrating()).as("Should not clear migrating flag").isTrue();
    }

    private static InternalPartitionImpl getInternalPartition(HecatonInstance member, int partitionId) {
        final NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(member);
        final InternalPartitionService partitionService = nodeEngineImpl.getPartitionService();
        final InternalPartitionImpl partition = (InternalPartitionImpl) partitionService.getPartition(partitionId);
        return partition;
    }

    private static MigrationInfo createMigrationInfo(HecatonInstance master, HecatonInstance nonMaster) {
        // master requests migration of partition owned by nonMaster to master
        MigrationInfo migration
                = new MigrationInfo(getPartitionId(nonMaster),
                PartitionReplica.from(nonMaster.getCluster().getLocalMember()),
                PartitionReplica.from(master.getCluster().getLocalMember()),
                0, 1, -1, 0);
        migration.setMaster(getAddress(master));
        migration.setInitialPartitionVersion(2);
        return migration;
    }
}
