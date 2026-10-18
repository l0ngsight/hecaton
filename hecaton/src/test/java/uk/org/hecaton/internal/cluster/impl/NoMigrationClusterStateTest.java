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

package uk.org.hecaton.internal.cluster.impl;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ConfigAccessor;
import uk.org.hecaton.config.ServiceConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.internal.partition.MigrationAwareService;
import uk.org.hecaton.internal.partition.PartitionMigrationEvent;
import uk.org.hecaton.internal.partition.PartitionReplicationEvent;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.test.AssertTask;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static uk.org.hecaton.instance.impl.TestUtil.terminateInstance;
import static uk.org.hecaton.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static uk.org.hecaton.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.Accessors.getPartitionService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NoMigrationClusterStateTest extends HecatonTestSupport {

    private final NoReplicationService service = new NoReplicationService();

    @Test
    public void rebalancing_shouldNotHappen_whenMemberLeaves() {
        Config config = newConfigWithMigrationAwareService();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance[] instances = factory.newInstances(config, 3);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        changeClusterStateEventually(instances[1], ClusterState.NO_MIGRATION);
        terminateInstance(instances[0]);

        HecatonInstance hz = factory.newHecatonInstance(config);

        assertTrueAllTheTime(new AssertTask() {
            Node node = getNode(hz);
            InternalPartitionService partitionService = node.getPartitionService();

            @Override
            public void run() {
                List<Integer> memberPartitions = partitionService.getMemberPartitions(node.getThisAddress());
                assertThat(memberPartitions).isEmpty();
                service.assertNoReplication();
            }
        }, 10);
    }

    @Test
    public void promotions_shouldHappen_whenMemberLeaves() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance[] instances = factory.newInstances(new Config(), 3);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        changeClusterStateEventually(instances[1], ClusterState.NO_MIGRATION);
        terminateInstance(instances[0]);

        assertClusterSizeEventually(2, instances[1]);
        assertAllPartitionsAreAssigned(instances[1], 1);

        assertClusterSizeEventually(2, instances[2]);
        assertAllPartitionsAreAssigned(instances[2], 1);

        assertTrueEventually(() -> assertEquals(getPartitionService(instances[1]).getPartitionStateStamp(),
                getPartitionService(instances[2]).getPartitionStateStamp()));
    }

    @Test
    public void lostPartitions_shouldBeAssigned_toAvailableMembers() {
        int clusterSize = MAX_REPLICA_COUNT + 3;
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance[] instances = factory.newInstances(new Config(), clusterSize);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        changeClusterStateEventually(instances[1], ClusterState.NO_MIGRATION);

        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            terminateInstance(instances[i]);
        }

        for (int i = MAX_REPLICA_COUNT; i < clusterSize; i++) {
            assertClusterSizeEventually(clusterSize - MAX_REPLICA_COUNT, instances[i]);
            assertAllPartitionsAreAssigned(instances[i], 1);
        }
    }

    @Test
    public void lostPartitions_shouldBeAssigned_toAvailableMembers_whenMembersRemovedWhenClusterPASSIVE() {
        int clusterSize = MAX_REPLICA_COUNT + 3;
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance[] instances = factory.newInstances(new Config(), clusterSize);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        changeClusterStateEventually(instances[1], ClusterState.PASSIVE);

        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            terminateInstance(instances[i]);
        }

        changeClusterStateEventually(instances[instances.length - 1], ClusterState.NO_MIGRATION);

        for (int i = MAX_REPLICA_COUNT; i < clusterSize; i++) {
            assertClusterSizeEventually(clusterSize - MAX_REPLICA_COUNT, instances[i]);
            assertAllPartitionsAreAssigned(instances[i], 1);
        }
    }

    @Test
    public void lostPartitions_shouldBeAssigned_toNewMembers() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance[] instances = factory.newInstances(new Config(), MAX_REPLICA_COUNT);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        changeClusterStateEventually(instances[1], ClusterState.NO_MIGRATION);

        HecatonInstance[] newInstances = factory.newInstances(new Config(), 3);

        for (HecatonInstance instance : newInstances) {
            assertClusterSizeEventually(MAX_REPLICA_COUNT + newInstances.length, instance);
        }

        for (HecatonInstance instance : instances) {
            terminateInstance(instance);
        }

        for (HecatonInstance instance : newInstances) {
            assertClusterSizeEventually(newInstances.length, instance);
            assertAllPartitionsAreAssigned(instance, newInstances.length);
        }

        long partitionStamp = getPartitionService(newInstances[0]).getPartitionStateStamp();
        for (HecatonInstance instance : newInstances) {
            assertEquals(partitionStamp, getPartitionService(instance).getPartitionStateStamp());
        }
    }

    private static void assertAllPartitionsAreAssigned(HecatonInstance instance, int replicaCount) {
        ClusterServiceImpl clusterService = getNode(instance).getClusterService();
        InternalPartitionService partitionService = getNode(instance).getPartitionService();

        assertTrueEventually(() -> {
            InternalPartition[] partitions = partitionService.getInternalPartitions();
            for (InternalPartition partition : partitions) {
                for (int i = 0; i < replicaCount; i++) {
                    Address owner = partition.getReplicaAddress(i);
                    assertNotNull(i + "th replica owner is null: " + partition, owner);
                    assertNotNull("No member for: " + owner, clusterService.getMember(owner));
                }
            }
        });
    }

    @Test
    public void backupReplication_shouldNotHappen_whenMemberLeaves() {
        Config config = newConfigWithMigrationAwareService();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance[] instances = factory.newInstances(config, 3);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        changeClusterStateEventually(instances[1], ClusterState.NO_MIGRATION);
        terminateInstance(instances[0]);

        assertClusterSizeEventually(2, instances[1], instances[2]);

        assertTrueAllTheTime(service::assertNoReplication, 10);
    }

    @Test
    public void rebalancing_shouldHappen_whenStateBecomesActive() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance[] instances = factory.newInstances(new Config(), 3);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        changeClusterStateEventually(instances[1], ClusterState.NO_MIGRATION);
        terminateInstance(instances[0]);

        assertClusterSizeEventually(2, instances[1], instances[2]);

        changeClusterStateEventually(instances[1], ClusterState.ACTIVE);
        assertAllPartitionsAreAssigned(instances[1], 2);
        assertAllPartitionsAreAssigned(instances[2], 2);
    }

    @Test
    public void rebalancing_shouldNotHappen_whenStateBecomesFrozen() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance[] instances = factory.newInstances(newConfigWithMigrationAwareService(), 3);
        warmUpPartitions(instances);
        waitAllForSafeState(instances);

        changeClusterStateEventually(instances[1], ClusterState.NO_MIGRATION);
        terminateInstance(instances[0]);

        assertClusterSizeEventually(2, instances[1], instances[2]);

        changeClusterStateEventually(instances[1], ClusterState.FROZEN);

        assertTrueAllTheTime(service::assertNoReplication, 10);
    }

    private Config newConfigWithMigrationAwareService() {
        Config config = new Config();
        ConfigAccessor.getServicesConfig(config).addServiceConfig(new ServiceConfig()
                .setEnabled(true)
                .setName("no-replication-service")
                .setImplementation(service));
        return config;
    }

    private static class NoReplicationService implements MigrationAwareService {

        private final AtomicReference<AssertionError> replicationRequested = new AtomicReference<>();

        @Override
        public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
            AssertionError error = new AssertionError("Replication requested: " + event);
            replicationRequested.compareAndSet(null, error);
            throw error;
        }

        @Override
        public void beforeMigration(PartitionMigrationEvent event) {
        }

        @Override
        public void commitMigration(PartitionMigrationEvent event) {
        }

        @Override
        public void rollbackMigration(PartitionMigrationEvent event) {
        }

        void assertNoReplication() {
            AssertionError error = replicationRequested.get();
            if (error != null) {
                throw error;
            }
        }
    }
}
