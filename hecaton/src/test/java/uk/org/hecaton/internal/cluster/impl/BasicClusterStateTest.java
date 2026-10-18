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
import uk.org.hecaton.cluster.Cluster;
import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.MemberLeftException;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.impl.NodeState;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.partition.IndeterminateOperationStateExceptionTest.BackupOperation;
import uk.org.hecaton.partition.IndeterminateOperationStateExceptionTest.SilentOperation;
import uk.org.hecaton.partition.PartitionLostListener;
import uk.org.hecaton.spi.impl.AllowedDuringPassiveState;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.eventservice.EventRegistration;
import uk.org.hecaton.spi.impl.eventservice.EventService;
import uk.org.hecaton.spi.impl.eventservice.impl.EventServiceImpl;
import uk.org.hecaton.spi.impl.operationservice.BackupAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.transaction.TransactionOptions;
import uk.org.hecaton.transaction.TransactionOptions.TransactionType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;

import static uk.org.hecaton.instance.impl.TestUtil.terminateInstance;
import static uk.org.hecaton.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static uk.org.hecaton.internal.partition.InternalPartitionService.PARTITION_LOST_EVENT_TOPIC;
import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static uk.org.hecaton.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class BasicClusterStateTest extends HecatonTestSupport {

    @Test
    public void clusterState_isActive_whenInstancesStarted() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        HecatonInstance[] instances = factory.newInstances();
        assertClusterState(ClusterState.ACTIVE, instances);
    }

    @Test
    public void joinNotAllowed_whenClusterState_isFrozen() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(4);
        HecatonInstance[] instances = new HecatonInstance[3];
        for (int i = 0; i < 3; i++) {
            instances[i] = factory.newHecatonInstance();
        }

        HecatonInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.FROZEN);

        Assert.assertThrows(IllegalStateException.class, factory::newHecatonInstance);
    }

    @Test
    public void joinNotAllowed_whenClusterState_isPassive() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(4);
        HecatonInstance[] instances = new HecatonInstance[3];
        for (int i = 0; i < 3; i++) {
            instances[i] = factory.newHecatonInstance();
        }

        HecatonInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.PASSIVE);

        Assert.assertThrows(IllegalStateException.class, factory::newHecatonInstance);
    }

    @Test
    public void joinAllowed_whenClusterState_isNoMigration() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(4);
        HecatonInstance[] instances = new HecatonInstance[3];
        for (int i = 0; i < 3; i++) {
            instances[i] = factory.newHecatonInstance();
        }

        instances[instances.length - 1].getCluster().changeClusterState(ClusterState.NO_MIGRATION);

        HecatonInstance hz = factory.newHecatonInstance();
        assertClusterSize(4, hz);
    }

    @Test
    public void joinAllowed_whenKnownMemberReJoins_whenClusterState_isFrozen() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(4);
        HecatonInstance[] instances = new HecatonInstance[3];
        for (int i = 0; i < 3; i++) {
            instances[i] = factory.newHecatonInstance();
        }

        HecatonInstance hz1 = instances[instances.length - 1];
        hz1.getCluster().changeClusterState(ClusterState.FROZEN);

        HecatonInstance hz2 = instances[0];
        Address address = getNode(hz2).getThisAddress();
        hz2.getLifecycleService().terminate();

        hz2 = factory.newHecatonInstance(address);

        assertClusterSizeEventually(3, hz1, hz2);
        assertEquals(NodeState.ACTIVE, getNode(hz2).getState());
    }

    @Test
    public void joinAllowed_whenKnownMemberReJoins_whenClusterState_isPassive() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(4);
        HecatonInstance[] instances = new HecatonInstance[3];
        for (int i = 0; i < 3; i++) {
            instances[i] = factory.newHecatonInstance();
        }

        HecatonInstance hz1 = instances[instances.length - 1];
        hz1.getCluster().changeClusterState(ClusterState.PASSIVE);

        HecatonInstance hz2 = instances[0];
        Address address = getNode(hz2).getThisAddress();
        hz2.getLifecycleService().terminate();

        hz2 = factory.newHecatonInstance(address);

        assertClusterSizeEventually(3, hz1, hz2);
        assertEquals(NodeState.PASSIVE, getNode(hz2).getState());
    }

    @Test
    public void changeClusterState_toNoMigration_shouldFail_whilePartitionsMigrating() {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_MIGRATION_INTERVAL.getName(), "10");

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance hz = factory.newHecatonInstance(config);
        warmUpPartitions(hz);

        HecatonInstance hz2 = factory.newHecatonInstance(config);
        Cluster cluster = hz2.getCluster();
        Assert.assertThrows(IllegalStateException.class, () -> cluster.changeClusterState(ClusterState.NO_MIGRATION));
    }

    @Test
    public void changeClusterState_toFrozen_shouldFail_whilePartitionsMigrating() {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_MIGRATION_INTERVAL.getName(), "10");

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance hz = factory.newHecatonInstance(config);
        warmUpPartitions(hz);

        HecatonInstance hz2 = factory.newHecatonInstance(config);
        Cluster cluster = hz2.getCluster();
        Assert.assertThrows(IllegalStateException.class, () -> cluster.changeClusterState(ClusterState.FROZEN));
    }

    @Test
    public void changeClusterState_toPassive_shouldFail_whilePartitionsMigrating() {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_MIGRATION_INTERVAL.getName(), "10");

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance hz = factory.newHecatonInstance(config);
        warmUpPartitions(hz);

        HecatonInstance hz2 = factory.newHecatonInstance(config);
        assertClusterSizeEventually(2, hz);
        Cluster cluster = hz2.getCluster();
        Assert.assertThrows(IllegalStateException.class, () -> cluster.changeClusterState(ClusterState.PASSIVE));
    }

    @Test
    public void changeClusterState_toActive_isAllowed_whileReplicationInProgress() {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_MIGRATION_INTERVAL.getName(), "10");
        config.setProperty(ClusterProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "1");

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        HecatonInstance[] instances = factory.newInstances(config);
        HecatonInstance hz1 = instances[0];
        HecatonInstance hz2 = instances[1];
        HecatonInstance hz3 = instances[2];
        warmUpPartitions(instances);

        changeClusterStateEventually(hz2, ClusterState.FROZEN);
        terminateInstance(hz1);
        assertClusterSizeEventually(2, hz2, hz3);

        // try until member is removed and partition-service takes care of removal
        changeClusterStateEventually(hz3, ClusterState.ACTIVE);

        assertClusterState(ClusterState.ACTIVE, hz2, hz3);
    }

    @Test
    public void changeClusterState_toPassive_isAllowed_whileReplicationInProgress() {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_MIGRATION_INTERVAL.getName(), "10");
        config.setProperty(ClusterProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "1");

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        HecatonInstance[] instances = factory.newInstances(config);
        HecatonInstance hz1 = instances[0];
        HecatonInstance hz2 = instances[1];
        HecatonInstance hz3 = instances[2];
        warmUpPartitions(instances);

        changeClusterStateEventually(hz2, ClusterState.FROZEN);
        terminateInstance(hz1);
        assertClusterSizeEventually(2, hz2, hz3);

        // try until member is removed and partition-service takes care of removal
        changeClusterStateEventually(hz3, ClusterState.PASSIVE);

        assertClusterState(ClusterState.PASSIVE, hz2, hz3);
    }

    @Test
    public void changeClusterState_toFrozen_makesNodeStates_Active() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        HecatonInstance[] instances = factory.newInstances();

        HecatonInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.FROZEN);
        assertNodeState(instances, NodeState.ACTIVE);
    }

    @Test
    public void changeClusterState_toPassive_makesNodeStates_Passive() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        HecatonInstance[] instances = factory.newInstances();

        HecatonInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.PASSIVE);
        assertNodeState(instances, NodeState.PASSIVE);
    }

    @Test
    public void changeClusterState_fromPassiveToActive_makesNodeStates_Active() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        HecatonInstance[] instances = factory.newInstances();

        HecatonInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.PASSIVE);
        hz.getCluster().changeClusterState(ClusterState.ACTIVE);
        assertNodeState(instances, NodeState.ACTIVE);
    }

    @Test
    public void changeClusterState_fromPassiveToFrozen_makesNodeStates_Active() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        HecatonInstance[] instances = factory.newInstances();

        HecatonInstance hz = instances[instances.length - 1];
        hz.getCluster().changeClusterState(ClusterState.PASSIVE);
        hz.getCluster().changeClusterState(ClusterState.ACTIVE);
        assertNodeState(instances, NodeState.ACTIVE);
    }

    @Test
    public void changeClusterState_transaction_mustBe_TWO_PHASE() {
        HecatonInstance hz = createHecatonInstance();
        TransactionOptions options = new TransactionOptions().setTransactionType(TransactionType.ONE_PHASE);

        Cluster cluster = hz.getCluster();
        Assert.assertThrows(IllegalArgumentException.class, () -> cluster.changeClusterState(ClusterState.FROZEN, options));
    }

    @Test
    public void readOperations_succeed_whenClusterState_passive() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        HecatonInstance[] instances = factory.newInstances();
        warmUpPartitions(instances);

        HecatonInstance hz = instances[instances.length - 1];
        Map<Object, Object> map = hz.getMap(randomMapName());
        changeClusterStateEventually(hz, ClusterState.PASSIVE);
        map.get(1);
    }

    @Test(timeout = 300000)
    public void test_noMigration_whenNodeLeaves_onClusterState_FROZEN() {
        testNoMigrationWhenNodeLeaves(ClusterState.FROZEN);
    }

    @Test(timeout = 300000)
    public void test_noMigration_whenNodeLeaves_onClusterState_PASSIVE() {
        testNoMigrationWhenNodeLeaves(ClusterState.PASSIVE);
    }

    private void testNoMigrationWhenNodeLeaves(final ClusterState clusterState) {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        HecatonInstance master = factory.newHecatonInstance();
        HecatonInstance other = factory.newHecatonInstance();

        IMap<Object, Object> map = master.getMap(randomMapName());
        for (int i = 0; i < 10000; i++) {
            map.put(i, i);
        }

        changeClusterStateEventually(master, clusterState);

        Address otherAddress = getAddress(other);

        other.shutdown();
        assertClusterSizeEventually(1, master);

        other = factory.newHecatonInstance(otherAddress);

        assertClusterSizeEventually(2, master, other);

        other.shutdown();
    }

    @Test
    public void test_listener_registration_whenClusterState_PASSIVE() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        HecatonInstance master = factory.newHecatonInstance();
        HecatonInstance other = factory.newHecatonInstance();

        changeClusterStateEventually(master, ClusterState.PASSIVE);
        master.getPartitionService().addPartitionLostListener(mock(PartitionLostListener.class));
        // Expected = 7 -> 1 added + 1 from {@link uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers * instances
        assertRegistrationsSizeEventually(master, InternalPartitionService.SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, 7);
        assertRegistrationsSizeEventually(other, InternalPartitionService.SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, 7);
    }

    @Test
    public void test_listener_deregistration_whenClusterState_PASSIVE() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        HecatonInstance master = factory.newHecatonInstance();
        HecatonInstance other = factory.newHecatonInstance();

        UUID registrationId = master.getPartitionService().addPartitionLostListener(mock(PartitionLostListener.class));
        // Expected = 7 -> 1 added + 1 from {@link uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers * instances
        assertRegistrationsSizeEventually(master, InternalPartitionService.SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, 7);
        assertRegistrationsSizeEventually(other, InternalPartitionService.SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, 7);

        changeClusterStateEventually(master, ClusterState.PASSIVE);
        master.getPartitionService().removePartitionLostListener(registrationId);
        // Expected = 6 -> see {@link uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService}
        // + 2 from map and cache ExpirationManagers* instances
        assertRegistrationsSizeEventually(other, InternalPartitionService.SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, 6);
        assertRegistrationsSizeEventually(master, InternalPartitionService.SERVICE_NAME, PARTITION_LOST_EVENT_TOPIC, 6);
    }

    @Test
    public void test_eventsDispatched_whenClusterState_PASSIVE() {
        System.setProperty(EventServiceImpl.EVENT_SYNC_FREQUENCY_PROP, "1");
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        HecatonInstance master = factory.newHecatonInstance();
        HecatonInstance other = factory.newHecatonInstance();

        changeClusterStateEventually(master, ClusterState.PASSIVE);
        master.getMap(randomMapName());

        assertTrueEventually(() -> assertEquals(1, getNodeEngineImpl(other).getProxyService().getProxyCount()));
    }

    @Test
    public void pendingInvocations_shouldBeNotified_whenMemberLeft_whenClusterState_PASSIVE() {
        pendingInvocations_shouldBeNotified_whenMemberLeft_whenClusterState_doesNotAllowJoin(ClusterState.PASSIVE);
    }

    @Test
    public void pendingInvocations_shouldBeNotified_whenMemberLeft_whenClusterState_FROZEN() {
        pendingInvocations_shouldBeNotified_whenMemberLeft_whenClusterState_doesNotAllowJoin(ClusterState.FROZEN);
    }

    private void pendingInvocations_shouldBeNotified_whenMemberLeft_whenClusterState_doesNotAllowJoin(ClusterState state) {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance hz1 = factory.newHecatonInstance();
        HecatonInstance hz2 = factory.newHecatonInstance();
        warmUpPartitions(hz1, hz2);

        Future<Object> future = getOperationService(hz2).invokeOnTarget(null, new SilentOperation(),
                getAddress(hz1));

        changeClusterStateEventually(hz2, state);
        hz1.shutdown();

        Assert.assertThrows(MemberLeftException.class, future::get);
    }

    @Test
    public void backupOperation_shouldBeAllowed_whenClusterState_PASSIVE() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance hz1 = factory.newHecatonInstance();
        HecatonInstance hz2 = factory.newHecatonInstance();
        warmUpPartitions(hz1, hz2);

        int partitionId = getPartitionId(hz2);
        changeClusterStateEventually(hz1, ClusterState.PASSIVE);

        InternalCompletableFuture<Object> future = getOperationService(hz1).invokeOnPartition(null,
                new PrimaryAllowedDuringPassiveStateOperation(), partitionId);
        future.join();

        assertTrueEventually(() -> assertTrue(hz1.getUserContext().containsKey(BackupOperation.EXECUTION_DONE)));
    }

    private static void assertNodeState(HecatonInstance[] instances, NodeState expectedState) {
        for (HecatonInstance instance : instances) {
            Node node = getNode(instance);
            assertEquals(expectedState, node.getState());
        }
    }

    private void assertRegistrationsSizeEventually(final HecatonInstance instance, final String serviceName, final String topic, final int size) {
        assertTrueEventually(() -> {
            EventService eventService = getNode(instance).getNodeEngine().getEventService();
            Collection<EventRegistration> registrations = eventService.getRegistrations(serviceName, topic);
            assertEquals(size, registrations.size());
        });
    }

    private static class PrimaryAllowedDuringPassiveStateOperation extends Operation
            implements BackupAwareOperation, AllowedDuringPassiveState {

        @Override
        public void run() {
        }

        @Override
        public boolean shouldBackup() {
            return true;
        }

        @Override
        public int getSyncBackupCount() {
            return 1;
        }

        @Override
        public int getAsyncBackupCount() {
            return 0;
        }

        @Override
        public Operation getBackupOperation() {
            return new BackupOperation();
        }
    }
}
