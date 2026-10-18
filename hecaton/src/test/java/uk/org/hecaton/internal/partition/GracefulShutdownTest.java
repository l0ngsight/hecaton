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

package uk.org.hecaton.internal.partition;

import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.cluster.impl.ClusterDataSerializerHook;
import uk.org.hecaton.internal.partition.impl.InternalPartitionServiceImpl;
import uk.org.hecaton.internal.partition.impl.MigrationInterceptor;
import uk.org.hecaton.internal.util.RandomPicker;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static uk.org.hecaton.instance.impl.TestUtil.terminateInstance;
import static uk.org.hecaton.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static uk.org.hecaton.internal.partition.AbstractPartitionAssignmentsCorrectnessTest.assertPartitionAssignments;
import static uk.org.hecaton.internal.partition.AbstractPartitionAssignmentsCorrectnessTest.assertPartitionAssignmentsEventually;
import static uk.org.hecaton.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.Accessors.getPartitionService;
import static uk.org.hecaton.test.PacketFiltersUtil.dropOperationsFrom;
import static uk.org.hecaton.test.PacketFiltersUtil.resetPacketFiltersFrom;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/*
 * When executed with HecatonParallelClassRunner, this test creates a massive amount of threads (peaks of 1000 threads
 * and 800 daemon threads). As comparison the BasicMapTest creates about 700 threads and 25 daemon threads.
 * This regularly results in test failures when multiple PR builders run in parallel due to a resource starvation.
 *
 * Countermeasures are to remove the ParallelJVMTest annotation or to use the HecatonSerialClassRunner.
 *
 * Without ParallelJVMTest we'll add the whole test duration to the PR builder time (about 25 seconds) and still create the
 * resource usage peak, which may have a negative impact on parallel PR builder runs on the same host machine.
 *
 * With HecatonSerialClassRunner the test takes over 3 minutes, but with a maximum of 200 threads and 160 daemon threads.
 * This should have less impact on other tests and the total duration of the PR build (since the test will still be executed
 * in parallel to others).
 */
@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GracefulShutdownTest extends HecatonTestSupport {

    private TestHecatonInstanceFactory factory;

    @Before
    public void setup() {
        factory = createHecatonInstanceFactory();
    }

    @Test
    public void shutdownSingleMember_withoutPartitionInitialization() {
        HecatonInstance hz = factory.newHecatonInstance();
        hz.shutdown();
    }

    @Test
    public void shutdownSingleMember_withPartitionInitialization() {
        HecatonInstance hz = factory.newHecatonInstance();
        warmUpPartitions(hz);
        hz.shutdown();
    }

    @Test
    public void shutdownSingleLiteMember() {
        HecatonInstance hz = factory.newHecatonInstance(new Config().setLiteMember(true));
        hz.shutdown();
    }

    @Test
    @SuppressWarnings("unused")
    public void shutdownSlaveMember_withoutPartitionInitialization() {
        HecatonInstance hz1 = factory.newHecatonInstance();
        HecatonInstance hz2 = factory.newHecatonInstance();
        HecatonInstance hz3 = factory.newHecatonInstance();

        hz2.shutdown();
    }

    @Test
    public void shutdownSlaveMember_withPartitionInitialization() {
        HecatonInstance hz1 = factory.newHecatonInstance();
        HecatonInstance hz2 = factory.newHecatonInstance();
        HecatonInstance hz3 = factory.newHecatonInstance();

        warmUpPartitions(hz1, hz2, hz3);
        hz2.shutdown();
        assertPartitionAssignmentsEventually(factory);
    }

    @Test
    @SuppressWarnings("unused")
    public void shutdownSlaveMember_whilePartitionsMigrating() {
        Config config = new Config()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "12")
                .setProperty(ClusterProperty.PARTITION_MIGRATION_INTERVAL.getName(), "1");

        HecatonInstance hz1 = factory.newHecatonInstance(config);
        warmUpPartitions(hz1);

        HecatonInstance hz2 = factory.newHecatonInstance(config);
        HecatonInstance hz3 = factory.newHecatonInstance(config);

        hz2.shutdown();

        assertPartitionAssignmentsEventually(factory);
    }

    @Test
    public void shutdownSlaveLiteMember() {
        HecatonInstance hz1 = factory.newHecatonInstance();
        HecatonInstance hz2 = factory.newHecatonInstance(new Config().setLiteMember(true));
        HecatonInstance hz3 = factory.newHecatonInstance();

        warmUpPartitions(hz1, hz2, hz3);
        hz2.shutdown();

        assertPartitionAssignments(factory);
    }

    @Test
    @SuppressWarnings("unused")
    public void shutdownMasterMember_withoutPartitionInitialization() {
        HecatonInstance hz1 = factory.newHecatonInstance();
        HecatonInstance hz2 = factory.newHecatonInstance();
        HecatonInstance hz3 = factory.newHecatonInstance();

        hz1.shutdown();
    }

    @Test
    public void shutdownMasterMember_withPartitionInitialization() {
        HecatonInstance hz1 = factory.newHecatonInstance();
        HecatonInstance hz2 = factory.newHecatonInstance();
        HecatonInstance hz3 = factory.newHecatonInstance();

        warmUpPartitions(hz1, hz2, hz3);
        hz1.shutdown();

        assertPartitionAssignmentsEventually(factory);
    }

    @Test
    @SuppressWarnings("unused")
    public void shutdownMasterMember_whilePartitionsMigrating() {
        Config config = newConfig();

        HecatonInstance hz1 = factory.newHecatonInstance(config);
        warmUpPartitions(hz1);

        HecatonInstance hz2 = factory.newHecatonInstance(config);
        HecatonInstance hz3 = factory.newHecatonInstance(config);

        hz1.shutdown();

        assertPartitionAssignmentsEventually(factory);
    }

    @Test
    public void shutdownMasterLiteMember() {
        HecatonInstance hz1 = factory.newHecatonInstance(new Config().setLiteMember(true));
        HecatonInstance hz2 = factory.newHecatonInstance();
        HecatonInstance hz3 = factory.newHecatonInstance();

        warmUpPartitions(hz1, hz2, hz3);
        hz1.shutdown();

        assertPartitionAssignments(factory);
    }

    @Test
    public void shutdownAllMembers_withoutPartitionInitialization() {
        shutdownAllMembers(false);
    }

    @Test
    public void shutdownAllMembers_withPartitionInitialization() {
        shutdownAllMembers(true);
    }

    private void shutdownAllMembers(boolean initializePartitions) {
        final HecatonInstance[] instances = factory.newInstances(new Config(), 4);

        if (initializePartitions) {
            warmUpPartitions(instances);
        }

        final CountDownLatch latch = new CountDownLatch(instances.length);
        for (final HecatonInstance instance : instances) {
            new Thread(() -> {
                instance.shutdown();
                latch.countDown();
            }).start();
        }

        assertOpenEventually(latch);
    }

    @Test
    public void shutdownMultipleSlaveMembers_withoutPartitionInitialization() {
        shutdownMultipleMembers(false, false);
    }

    @Test
    public void shutdownMultipleMembers_withoutPartitionInitialization() {
        shutdownMultipleMembers(true, false);
    }

    @Test
    public void shutdownMultipleSlaveMembers_withPartitionInitialization() {
        shutdownMultipleMembers(false, true);
    }

    @Test
    public void shutdownMultipleMembers_withPartitionInitialization() {
        shutdownMultipleMembers(true, true);
    }

    private void shutdownMultipleMembers(boolean includeMaster, boolean initializePartitions) {
        final HecatonInstance[] instances = factory.newInstances(new Config(), 6);

        if (initializePartitions) {
            warmUpPartitions(instances);
        }

        final CountDownLatch latch = new CountDownLatch(instances.length / 2);
        int startIndex = includeMaster ? 0 : 1;
        for (int i = startIndex; i < instances.length; i += 2) {
            final int index = i;
            new Thread(() -> {
                instances[index].shutdown();
                latch.countDown();
            }).start();
        }

        assertOpenEventually(latch);

        if (initializePartitions) {
            assertPartitionAssignmentsEventually(factory);
        }
    }

    @Test
    public void shutdownMultipleMembers_whilePartitionsMigrating() {
        Config config = newConfig();

        HecatonInstance master = factory.newHecatonInstance(config);
        warmUpPartitions(master);

        HecatonInstance[] slaves = factory.newInstances(config, 5);

        final List<HecatonInstance> instances = new ArrayList<>(slaves.length + 1);
        instances.add(master);
        instances.addAll(Arrays.asList(slaves));
        Collections.shuffle(instances);

        final int count = instances.size() / 2;
        final CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            final int index = i;
            new Thread(() -> {
                HecatonInstance instance = instances.get(index);
                instance.shutdown();
                latch.countDown();
            }).start();
        }

        assertOpenEventually(latch);
        assertPartitionAssignmentsEventually(factory);
    }

    @Test
    public void shutdownAndTerminateSlaveMembers_concurrently() {
        HecatonInstance[] instances = factory.newInstances(new Config(), 5);
        int shutdownIndex = RandomPicker.getInt(1, instances.length);
        int terminateIndex;
        do {
            terminateIndex = RandomPicker.getInt(1, instances.length);
        } while (terminateIndex == shutdownIndex);

        shutdownAndTerminateMembers_concurrently(instances, shutdownIndex, terminateIndex);
    }

    @Test
    public void shutdownMasterAndTerminateSlaveMember_concurrently() {
        HecatonInstance[] instances = factory.newInstances(new Config(), 5);
        int shutdownIndex = 0;
        int terminateIndex = RandomPicker.getInt(1, instances.length);

        shutdownAndTerminateMembers_concurrently(instances, shutdownIndex, terminateIndex);
    }

    @Test
    public void shutdownSlaveAndTerminateMasterMember_concurrently() {
        HecatonInstance[] instances = factory.newInstances(new Config(), 5);
        int shutdownIndex = RandomPicker.getInt(1, instances.length);
        int terminateIndex = 0;

        shutdownAndTerminateMembers_concurrently(instances, shutdownIndex, terminateIndex);
    }

    private void shutdownAndTerminateMembers_concurrently(HecatonInstance[] instances, int shutdownIndex, int terminateIndex) {
        warmUpPartitions(instances);

        final HecatonInstance shuttingDownInstance = instances[shutdownIndex];
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            shuttingDownInstance.shutdown();
            latch.countDown();
        }).start();

        // spin until node starts to shut down
        Node shuttingDownNode = getNode(shuttingDownInstance);
        while (shuttingDownNode.isRunning()) {
            Thread.yield();
        }

        terminateInstance(instances[terminateIndex]);

        assertOpenEventually(latch);
        assertPartitionAssignmentsEventually(factory);
    }

    @Test
    public void shutdownMasterMember_whenClusterFrozen_withoutPartitionInitialization() {
        shutdownMember_whenClusterNotActive(true, false, ClusterState.FROZEN);
    }

    @Test
    public void shutdownMasterMember_whenClusterPassive_withoutPartitionInitialization() {
        shutdownMember_whenClusterNotActive(true, false, ClusterState.PASSIVE);
    }

    @Test
    public void shutdownMasterMember_whenClusterFrozen_withPartitionInitialization() {
        shutdownMember_whenClusterNotActive(true, true, ClusterState.FROZEN);
    }

    @Test
    public void shutdownMasterMember_whenClusterPassive_withPartitionInitialization() {
        shutdownMember_whenClusterNotActive(true, true, ClusterState.PASSIVE);
    }

    @Test
    public void shutdownSlaveMember_whenClusterFrozen_withoutPartitionInitialization() {
        shutdownMember_whenClusterNotActive(false, false, ClusterState.FROZEN);
    }

    @Test
    public void shutdownSlaveMember_whenClusterPassive_withoutPartitionInitialization() {
        shutdownMember_whenClusterNotActive(false, false, ClusterState.PASSIVE);
    }

    @Test
    public void shutdownSlaveMember_whenClusterFrozen_withPartitionInitialization() {
        shutdownMember_whenClusterNotActive(false, true, ClusterState.FROZEN);
    }

    @Test
    public void shutdownSlaveMember_whenClusterPassive_withPartitionInitialization() {
        shutdownMember_whenClusterNotActive(false, true, ClusterState.PASSIVE);
    }

    private void shutdownMember_whenClusterNotActive(boolean shutdownMaster, boolean initializePartitions, ClusterState state) {
        Config config = new Config();
        HecatonInstance master = factory.newHecatonInstance(config);
        HecatonInstance[] slaves = factory.newInstances(config, 3);

        if (initializePartitions) {
            warmUpPartitions(slaves);
        }

        changeClusterStateEventually(slaves[0], state);

        InternalPartition[] partitionsBefore = getPartitionTable(master);

        if (shutdownMaster) {
            master.shutdown();
        } else {
            slaves[0].shutdown();
        }

        InternalPartition[] partitionsAfter = getPartitionTable(slaves[slaves.length - 1]);
        assertPartitionTableEquals(partitionsBefore, partitionsAfter);
    }

    @Test
    public void shutdownMemberAndCluster_withoutPartitionInitialization() {
        shutdownMemberAndCluster(false);
    }

    @Test
    public void shutdownMemberAndCluster_withPartitionInitialization() {
        shutdownMemberAndCluster(true);
    }

    private void shutdownMemberAndCluster(boolean initializePartitions) {
        Config config = new Config();
        HecatonInstance master = factory.newHecatonInstance(config);
        HecatonInstance[] slaves = factory.newInstances(config, 3);

        if (initializePartitions) {
            warmUpPartitions(master);
        }

        master.shutdown();

        changeClusterStateEventually(slaves[0], ClusterState.PASSIVE);
        slaves[0].getCluster().shutdown();
    }

    @Test
    public void shutdownMemberAndCluster_concurrently_withoutPartitionInitialization() throws Exception {
        shutdownMemberAndCluster_concurrently(false);
    }

    @Test
    public void shutdownMemberAndCluster_concurrently_withPartitionInitialization() throws Exception {
        shutdownMemberAndCluster_concurrently(true);
    }

    private void shutdownMemberAndCluster_concurrently(boolean initializePartitions) throws Exception {
        Config config = new Config();

        final HecatonInstance master = factory.newHecatonInstance(config);
        final HecatonInstance[] slaves = factory.newInstances(config, 3);

        if (initializePartitions) {
            warmUpPartitions(master);
        }

        Future<?> f1 = spawn((Runnable) master::shutdown);

        Future<?> f2 = spawn((Runnable) () -> {
            changeClusterStateEventually(slaves[0], ClusterState.PASSIVE);
            slaves[0].getCluster().shutdown();
        });

        f1.get();
        f2.get();
    }

    @Test
    public void shutdownMasterCandidate_whileMastershipClaimIsInProgress() throws Exception {
        Config config = new Config();
        // setting a very graceful shutdown high timeout value
        // to guarantee instance.shutdown() not to timeout
        config.setProperty(ClusterProperty.GRACEFUL_SHUTDOWN_MAX_WAIT.getName(), "99999999999");

        final HecatonInstance[] instances = factory.newInstances(config, 4);
        assertClusterSizeEventually(4, instances);
        warmUpPartitions(instances);

        // Drop mastership claim operation submitted from master candidate
        dropOperationsFrom(instances[1], ClusterDataSerializerHook.F_ID, singletonList(ClusterDataSerializerHook.FETCH_MEMBER_LIST_STATE));

        final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instances[1]);
        final AtomicReference<MigrationInfo> startedMigration = new AtomicReference<>();
        partitionService.setMigrationInterceptor(new MigrationInterceptor() {
            @Override
            public void onMigrationStart(MigrationParticipant participant, MigrationInfo migrationInfo) {
                startedMigration.set(migrationInfo);
            }
        });

        final long partitionStateStamp = partitionService.getPartitionStateStamp();

        instances[0].getLifecycleService().terminate();

        // instance-1 starts mastership claim
        assertTrueEventually(() -> assertTrue(getNode(instances[1]).isMaster()));

        Future<?> future = spawn((Runnable) () -> instances[1].shutdown());

        assertTrueAllTheTime(() -> {
            // other members have not received/accepted mastership claim yet
            assertNotEquals(getAddress(instances[1]), getNode(instances[2]).getMasterAddress());
            assertNotEquals(getAddress(instances[1]), getNode(instances[3]).getMasterAddress());

            // no partition state version change
            assertEquals(partitionStateStamp, partitionService.getPartitionStateStamp());

            // no migrations have been submitted yet
            assertNull(startedMigration.get());
        }, 5);
        assertFalse(future.isDone());

        resetPacketFiltersFrom(instances[1]);
        future.get();
    }

    private static void assertPartitionTableEquals(InternalPartition[] partitions1, InternalPartition[] partitions2) {
        assertEquals(partitions1.length, partitions2.length);

        for (int i = 0; i < partitions1.length; i++) {
            assertPartitionEquals(partitions1[i], partitions2[i]);
        }
    }

    private static void assertPartitionEquals(InternalPartition partition1, InternalPartition partition2) {
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            PartitionReplica replica1 = partition1.getReplica(i);
            PartitionReplica replica2 = partition2.getReplica(i);

            if (replica1 == null) {
                assertNull(replica2);
            } else {
                assertEquals(replica1, replica2);
            }
        }
    }

    private static InternalPartition[] getPartitionTable(HecatonInstance instance) {
        InternalPartitionServiceImpl partitionService = getNode(instance).partitionService;
        return partitionService.getPartitionStateManager().getPartitionsCopy(true);
    }

    private static Config newConfig() {
        return new Config()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "6")
                .setProperty(ClusterProperty.PARTITION_MIGRATION_INTERVAL.getName(), "1");
    }
}
