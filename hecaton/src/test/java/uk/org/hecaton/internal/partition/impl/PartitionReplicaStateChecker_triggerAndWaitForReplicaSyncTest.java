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
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.internal.partition.PartitionReplica;
import uk.org.hecaton.internal.partition.ReadonlyInternalPartition;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.internal.partition.impl.PartitionServiceState.MIGRATION_LOCAL;
import static uk.org.hecaton.internal.partition.impl.PartitionServiceState.MIGRATION_ON_MASTER;
import static uk.org.hecaton.internal.partition.impl.PartitionServiceState.REPLICA_NOT_OWNED;
import static uk.org.hecaton.internal.partition.impl.PartitionServiceState.REPLICA_NOT_SYNC;
import static uk.org.hecaton.internal.partition.impl.PartitionServiceState.SAFE;
import static uk.org.hecaton.logging.Logger.getLogger;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionReplicaStateChecker_triggerAndWaitForReplicaSyncTest extends HecatonTestSupport {

    private final List<InternalPartition> partitions = new ArrayList<>();

    private NodeEngine nodeEngine;
    private PartitionStateManager partitionStateManager;
    private MigrationManager migrationManager;

    private PartitionReplicaStateChecker replicaStateChecker;

    @Before
    public void setUp() {
        ILogger logger = getLogger(PartitionReplicaStateChecker_triggerAndWaitForReplicaSyncTest.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterState()).thenReturn(ClusterState.ACTIVE);

        nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getLogger(any(Class.class))).thenReturn(logger);
        when(nodeEngine.getClusterService()).thenReturn(clusterService);

        partitionStateManager = mock(PartitionStateManager.class);
        when(partitionStateManager.getPartitions()).thenAnswer(
                (Answer<InternalPartition[]>) invocationOnMock -> {
                    InternalPartition[] partitionsArray = new InternalPartition[partitions.size()];
                    return partitions.toArray(partitionsArray);
                }
        );

        migrationManager = mock(MigrationManager.class);

        InternalPartitionService partitionService = mock(InternalPartitionService.class);
        when(partitionService.getPartitionStateManager()).thenReturn(partitionStateManager);
        when(partitionService.getMigrationManager()).thenReturn(migrationManager);

        replicaStateChecker = new PartitionReplicaStateChecker(nodeEngine,  partitionService);
    }

    @Test
    public void whenCalledWithZeroTimeout_thenDoNothing() {
        assertFalse(replicaStateChecker.triggerAndWaitForReplicaSync(0, TimeUnit.MILLISECONDS));
    }

    @Test
    public void whenHasMissingReplicaOwners_withAddress_thenWaitForMissingReplicaOwners() throws Exception {
        configureNeedsReplicaStateCheckResponse();

        Address address = new Address("127.0.0.1", 5701);
        PartitionReplica replica = new PartitionReplica(address, UuidUtil.newUnsecureUUID());
        InternalPartition partition = new ReadonlyInternalPartition(new PartitionReplica[]{replica}, 1, 0);
        partitions.add(partition);

        assertEquals(REPLICA_NOT_OWNED, replicaStateChecker.getPartitionServiceState());
        assertFalse(replicaStateChecker.triggerAndWaitForReplicaSync(10, TimeUnit.MILLISECONDS, 5));
    }

    @Test
    public void whenHasMissingReplicaOwners_withoutAddress_thenWaitForMissingReplicaOwners() {
        configureNeedsReplicaStateCheckResponse();

        InternalPartition partition = new ReadonlyInternalPartition(new PartitionReplica[0], 1, 0);
        partitions.add(partition);

        assertEquals(REPLICA_NOT_OWNED, replicaStateChecker.getPartitionServiceState());
        assertFalse(replicaStateChecker.triggerAndWaitForReplicaSync(10, TimeUnit.MILLISECONDS, 5));
    }

    @Test
    public void whenHasOngoingMigration_withLocalMigration_thenWaitForOngoingMigrations() {
        when(migrationManager.hasOnGoingMigration()).thenReturn(true);

        assertEquals(MIGRATION_LOCAL, replicaStateChecker.getPartitionServiceState());
        assertFalse(replicaStateChecker.triggerAndWaitForReplicaSync(10, TimeUnit.MILLISECONDS, 5));
    }

    @Test
    public void whenHasOngoingMigration_withMigrationOnMaster_thenWaitForOngoingMigrations() {
        when(nodeEngine.getMasterAddress()).thenReturn(null);
        when(nodeEngine.getClusterService().isJoined()).thenReturn(true);

        assertEquals(MIGRATION_ON_MASTER, replicaStateChecker.getPartitionServiceState());
        assertFalse(replicaStateChecker.triggerAndWaitForReplicaSync(10, TimeUnit.MILLISECONDS, 5));
    }

    @Test
    public void whenCheckAndTriggerReplicaSync() {
        configureNeedsReplicaStateCheckResponseOnEachSecondCall();

        InternalPartition partition = new ReadonlyInternalPartition(new PartitionReplica[]{null}, 1, 0);
        partitions.add(partition);

        assertEquals(REPLICA_NOT_SYNC, replicaStateChecker.getPartitionServiceState());
        assertFalse(replicaStateChecker.triggerAndWaitForReplicaSync(10, TimeUnit.MILLISECONDS, 5));
    }

    @Test
    public void whenCheckAndTriggerReplicaSync_withNeedsReplicaStateCheck_thenReturnTrue() {
        configureNeedsReplicaStateCheckResponseOnEachSecondCall();

        assertEquals(SAFE, replicaStateChecker.getPartitionServiceState());
        assertTrue(replicaStateChecker.triggerAndWaitForReplicaSync(10, TimeUnit.MILLISECONDS, 5));
    }

    @Test
    public void whenCheckAndTriggerReplicaSync_withoutNeedsReplicaStateCheck_thenReturnTrue() {
        assertEquals(SAFE, replicaStateChecker.getPartitionServiceState());
        assertTrue(replicaStateChecker.triggerAndWaitForReplicaSync(10, TimeUnit.MILLISECONDS, 5));
    }

    private void configureNeedsReplicaStateCheckResponse() {
        when(partitionStateManager.isInitialized()).thenReturn(true);
        when(partitionStateManager.getMemberGroupsSize()).thenReturn(1);
    }

    private void configureNeedsReplicaStateCheckResponseOnEachSecondCall() {
        when(partitionStateManager.isInitialized()).thenAnswer(new AlternatingAnswer());
        when(partitionStateManager.getMemberGroupsSize()).thenReturn(1);
    }

    /**
     * Alternately returns {@code true} and {@code false}, beginning with {@code false}.
     */
    private static class AlternatingAnswer implements Answer<Boolean> {

        private boolean state = true;

        @Override
        public Boolean answer(InvocationOnMock invocationOnMock) {
            state = !state;
            return state;
        }
    }
}
