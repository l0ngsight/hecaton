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
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.cluster.impl.operations.DemoteDataMemberOp;
import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.operationservice.impl.Invocation;
import uk.org.hecaton.spi.impl.operationservice.impl.InvocationRegistry;
import uk.org.hecaton.spi.impl.operationservice.impl.OperationServiceImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.test.annotation.SerializationSamplesExcluded;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;

import static uk.org.hecaton.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static uk.org.hecaton.internal.cluster.impl.ClusterDataSerializerHook.DEMOTE_DATA_MEMBER;
import static uk.org.hecaton.internal.cluster.impl.ClusterDataSerializerHook.EXPLICIT_SUSPICION;
import static uk.org.hecaton.internal.cluster.impl.ClusterDataSerializerHook.F_ID;
import static uk.org.hecaton.internal.cluster.impl.ClusterDataSerializerHook.HEARTBEAT;
import static uk.org.hecaton.internal.cluster.impl.ClusterDataSerializerHook.MEMBER_INFO_UPDATE;
import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.Accessors.getOperationService;
import static uk.org.hecaton.test.Accessors.getPartitionService;
import static uk.org.hecaton.test.PacketFiltersUtil.dropOperationsBetween;
import static uk.org.hecaton.test.PacketFiltersUtil.dropOperationsFrom;
import static uk.org.hecaton.test.PacketFiltersUtil.rejectOperationsBetween;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class, SerializationSamplesExcluded.class})
public class DemoteDataMemberTest extends HecatonTestSupport {

    @Test
    public void dataMaster_demoted() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config());
        HecatonInstance hz2 = factory.newHecatonInstance(new Config().setLiteMember(true));
        HecatonInstance hz3 = factory.newHecatonInstance(new Config().setLiteMember(true));

        assertThatThrownBy(() -> hz1.getCluster().demoteLocalDataMember())
                .isInstanceOf(IllegalStateException.class);

        assertFalse(getMember(hz1).isLiteMember());
        assertTrue(getMember(hz2).isLiteMember());
        assertTrue(getMember(hz3).isLiteMember());
    }

    @Test
    public void dataMember_demoted() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config().setLiteMember(true));
        HecatonInstance hz2 = factory.newHecatonInstance(new Config());
        HecatonInstance hz3 = factory.newHecatonInstance(new Config().setLiteMember(true));

        assertThatThrownBy(() -> hz2.getCluster().demoteLocalDataMember())
                .isInstanceOf(IllegalStateException.class);

        assertTrue(getMember(hz1).isLiteMember());
        assertFalse(getMember(hz2).isLiteMember());
        assertTrue(getMember(hz3).isLiteMember());
    }

    @Test
    public void liteMember_demotion_shouldFail_onLocal() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config().setLiteMember(true));
        factory.newHecatonInstance(new Config().setLiteMember(true));

        assertThatThrownBy(() -> hz1.getCluster().demoteLocalDataMember())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void liteMember_demotion_shouldFail_onNonMaster() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        factory.newHecatonInstance(new Config().setLiteMember(true));
        HecatonInstance hz2 = factory.newHecatonInstance(new Config().setLiteMember(true));
        HecatonInstance hz3 = factory.newHecatonInstance(new Config().setLiteMember(true));

        DemoteDataMemberOp op = new DemoteDataMemberOp();
        op.setCallerUuid(getMember(hz2).getUuid());

        InternalCompletableFuture<MembersView> future =
                getOperationService(hz2).invokeOnTarget(ClusterServiceImpl.SERVICE_NAME, op, getAddress(hz3));
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .rootCause().isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void liteMember_demotion_shouldBeNoop_onMaster() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config().setLiteMember(true));
        HecatonInstance hz2 = factory.newHecatonInstance(new Config().setLiteMember(true));

        DemoteDataMemberOp op = new DemoteDataMemberOp();
        op.setCallerUuid(getMember(hz2).getUuid());

        InternalCompletableFuture<MembersView> future =
                getOperationService(hz2).invokeOnTarget(ClusterServiceImpl.SERVICE_NAME, op, getAddress(hz1));
        future.join();
    }

    @Test
    public void notExistingMember_demotion_shouldFail() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config());
        HecatonInstance hz2 = factory.newHecatonInstance(new Config());

        DemoteDataMemberOp op = new DemoteDataMemberOp();
        op.setCallerUuid(UuidUtil.newUnsecureUUID());

        InternalCompletableFuture<MembersView> future =
                getOperationService(hz2).invokeOnTarget(ClusterServiceImpl.SERVICE_NAME, op, getAddress(hz1));
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .rootCause().isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void standaloneDataMember_demoted() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz = factory.newHecatonInstance(new Config());

        assertThatThrownBy(() -> hz.getCluster().demoteLocalDataMember())
                .isInstanceOf(IllegalStateException.class);

        assertFalse(getMember(hz).isLiteMember());
    }

    @Test
    public void demotedMasterDataMember_shouldHave_partitionsMigrated() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config());
        HecatonInstance hz2 = factory.newHecatonInstance(new Config());
        HecatonInstance hz3 = factory.newHecatonInstance(new Config());

        warmUpPartitions(hz1, hz2, hz3);
        assertPartitionsAssigned(hz1);

        hz1.getCluster().demoteLocalDataMember();

        assertNoPartitionsAssigned(hz1);
        waitAllForSafeState(Arrays.asList(hz1, hz2, hz3));

        long partitionStamp = getPartitionService(hz1).getPartitionStateStamp();
        assertEquals(partitionStamp, getPartitionService(hz2).getPartitionStateStamp());
        assertEquals(partitionStamp, getPartitionService(hz3).getPartitionStateStamp());
    }

    @Test
    public void demotedDataMember_shouldHave_partitionsMigrated() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config());
        HecatonInstance hz2 = factory.newHecatonInstance(new Config());
        HecatonInstance hz3 = factory.newHecatonInstance(new Config());

        warmUpPartitions(hz1, hz2, hz3);
        assertPartitionsAssigned(hz2);

        hz2.getCluster().demoteLocalDataMember();

        assertNoPartitionsAssignedEventually(hz2);

        waitAllForSafeState(hz1, hz2, hz3);

        long partitionStamp = getPartitionService(hz1).getPartitionStateStamp();
        assertEquals(partitionStamp, getPartitionService(hz2).getPartitionStateStamp());
        assertEquals(partitionStamp, getPartitionService(hz3).getPartitionStateStamp());
    }

    @Test
    public void dataMember_shouldFailWhenOnlyMember() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config());

        int entryCount = 100;
        String mapName = randomMapName();
        IMap<String, String> testMap = hz1.getMap(mapName);
        for (int i = 0; i < entryCount; ++i) {
            testMap.put("key" + i, "value" + i);
        }
        assertEquals(entryCount, testMap.size());
        assertPartitionsAssigned(hz1);

        assertThatThrownBy(() -> hz1.getCluster().demoteLocalDataMember())
                .isInstanceOf(IllegalStateException.class);

        assertPartitionsAssigned(hz1);
        assertFalse(getMember(hz1).isLiteMember());
    }

    @Test
    public void dataMember_shouldFailWhenLastDataMember() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config().setLiteMember(true));
        HecatonInstance hz2 = factory.newHecatonInstance(new Config());
        HecatonInstance hz3 = factory.newHecatonInstance(new Config().setLiteMember(true));

        warmUpPartitions(hz1, hz2, hz3);
        assertPartitionsAssigned(hz2);

        assertTrue(hz1.getPartitionService().isLocalMemberSafe());
        assertTrue(hz1.getPartitionService().isClusterSafe());

        assertThatThrownBy(() -> hz2.getCluster().demoteLocalDataMember())
                .isInstanceOf(IllegalStateException.class);

        assertPartitionsAssigned(hz2);
        assertFalse(getMember(hz2).isLiteMember());
    }

    @Test
    public void demotion_shouldFail_whenClusterStatePassive() {
        demotion_shouldFail_whenClusterState_NotAllowMigration(ClusterState.PASSIVE);
    }

    @Test
    public void demotion_shouldFail_whenClusterStateFrozen() {
        demotion_shouldFail_whenClusterState_NotAllowMigration(ClusterState.FROZEN);
    }

    @Test
    public void demotion_shouldFail_whenClusterStateNoMigration() {
        demotion_shouldFail_whenClusterState_NotAllowMigration(ClusterState.NO_MIGRATION);
    }

    private void demotion_shouldFail_whenClusterState_NotAllowMigration(ClusterState state) {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config());
        HecatonInstance hz2 = factory.newHecatonInstance(new Config());
        HecatonInstance hz3 = factory.newHecatonInstance(new Config());

        warmUpPartitions(hz1, hz2, hz3);
        changeClusterStateEventually(hz2, state);

        assertThatThrownBy(() -> hz2.getCluster().demoteLocalDataMember())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void demotion_shouldFail_whenMastershipClaimInProgress_duringDemotion() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config());
        HecatonInstance hz2 = factory.newHecatonInstance(new Config());

        // artificially set mastership claim flag
        ClusterServiceImpl clusterService = getNode(hz1).getClusterService();
        clusterService.getClusterJoinManager().setMastershipClaimInProgress();

        Cluster cluster = hz2.getCluster();
        assertThatThrownBy(cluster::demoteLocalDataMember)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void demotion_shouldSucceed_whenMasterLeaves_duringDemotion() throws Exception {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config());
        HecatonInstance hz2 = factory.newHecatonInstance(new Config());
        HecatonInstance hz3 = factory.newHecatonInstance(new Config());

        assertClusterSizeEventually(3, hz2);

        warmUpPartitions(hz1, hz2, hz3);
        assertPartitionsAssigned(hz3);

        dropOperationsBetween(hz3, hz1, F_ID, singletonList(DEMOTE_DATA_MEMBER));
        Cluster cluster = hz3.getCluster();
        Future<Exception> future = spawn(() -> {
            try {
                cluster.demoteLocalDataMember();
            } catch (Exception e) {
                return e;
            }
            return null;
        });
        assertDemotionInvocationStarted(hz3);

        hz1.getLifecycleService().terminate();
        assertClusterSizeEventually(2, hz2, hz3);

        Exception exception = future.get();
        assertNull(exception);
        assertTrue(getMember(hz3).isLiteMember());
        assertNoPartitionsAssigned(hz3);
    }

    @Test
    public void demotion_shouldSucceed_whenMasterIsSuspected_duringDemotion() throws Exception {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config());
        HecatonInstance hz2 = factory.newHecatonInstance(new Config());
        HecatonInstance hz3 = factory.newHecatonInstance(new Config());

        assertClusterSizeEventually(3, hz2);

        rejectOperationsBetween(hz3, hz1, F_ID, asList(DEMOTE_DATA_MEMBER, EXPLICIT_SUSPICION));
        dropOperationsFrom(hz2, F_ID, asList(MEMBER_INFO_UPDATE, EXPLICIT_SUSPICION));
        dropOperationsFrom(hz1, F_ID, singletonList(HEARTBEAT));

        Cluster cluster = hz3.getCluster();
        Future<?> future = spawn(cluster::demoteLocalDataMember);

        assertDemotionInvocationStarted(hz3);

        suspectMember(getNode(hz3), getNode(hz1));
        suspectMember(getNode(hz2), getNode(hz1));

        assertMasterAddressEventually(getAddress(hz2), hz3);

        dropOperationsBetween(hz3, hz1, F_ID, singletonList(EXPLICIT_SUSPICION));

        future.get();

        assertTrue(hz3.getCluster().getLocalMember().isLiteMember());
        assertNoPartitionsAssigned(hz3);
    }

    @Test
    public void test_data_member_demotion_causes_no_data_loss_on_three_members() {
        int entryCount = 1000;

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        Config config = new Config();

        // start first hecaton instance as a data member
        HecatonInstance firstHecatonInstance = factory.newHecatonInstance(config);

        // start second and third hecaton instances as a data member
        HecatonInstance secondHecatonInstance = factory.newHecatonInstance(config);
        HecatonInstance thirdHecatonInstance = factory.newHecatonInstance(config);

        // check if cluster is in a good shape
        assertTrueEventually(() -> assertTrue(firstHecatonInstance.getPartitionService().isClusterSafe()));

        // insert some dummy data into the testing map
        String mapName = randomMapName();
        IMap<String, String> testMap = firstHecatonInstance.getMap(mapName);
        for (int i = 0; i < entryCount; ++i) {
            testMap.put("key" + i, "value" + i);
        }

        // check all data is correctly inserted
        assertEquals(entryCount, testMap.size());

        // demote second instance
        secondHecatonInstance.getCluster().demoteLocalDataMember();

        // backup count for the map is set to 1
        // even with 1 node down, no data loss is expected
        assertTrueEventually(() -> assertEquals(entryCount, firstHecatonInstance.getMap(mapName).size()));
        assertTrueEventually(() -> assertEquals(entryCount, thirdHecatonInstance.getMap(mapName).size()));
    }

    @Test
    public void test_data_member_demotion_causes_no_data_loss_on_two_members() {
        int entryCount = 1000;

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        Config config = new Config();

        // start first hecaton instance as a data member
        HecatonInstance firstHecatonInstance = factory.newHecatonInstance(config);

        // start second hecaton instance as a data member
        HecatonInstance secondHecatonInstance = factory.newHecatonInstance(config);

        // check if cluster is in a good shape
        assertTrueEventually(() -> assertTrue(firstHecatonInstance.getPartitionService().isClusterSafe()));

        // insert some dummy data into the testing map
        String mapName = randomMapName();
        IMap<String, String> testMap = firstHecatonInstance.getMap(mapName);
        for (int i = 0; i < entryCount; ++i) {
            testMap.put("key" + i, "value" + i);
        }

        // check all data is correctly inserted
        assertEquals(entryCount, testMap.size());

        // kill second instance
        secondHecatonInstance.getCluster().demoteLocalDataMember();

        // backup count for the map is set to 1
        // even with 1 node down, no data loss is expected
        assertTrueEventually(() -> assertEquals(entryCount, firstHecatonInstance.getMap(mapName).size()));
    }

    private void assertDemotionInvocationStarted(HecatonInstance instance) {
        OperationServiceImpl operationService = getNode(instance).getNodeEngine().getOperationService();
        InvocationRegistry invocationRegistry = operationService.getInvocationRegistry();

        assertTrueEventually(() -> {
            for (Map.Entry<Long, Invocation> entry : invocationRegistry.entrySet()) {
                if (entry.getValue().op instanceof DemoteDataMemberOp) {
                    return;
                }
            }
            fail("Cannot find DemoteDataMemberOp invocation!");
        });
    }

    private static void assertPartitionsAssigned(HecatonInstance instance) {
        Address address = getAddress(instance);
        InternalPartition[] partitions = getPartitionService(instance).getInternalPartitions();

        int k = 0;
        for (InternalPartition partition : partitions) {
            if (address.equals(partition.getOwnerOrNull())) {
                k++;
            }
        }
        assertThat(k).isPositive();
    }

    private static void assertNoPartitionsAssigned(HecatonInstance instance) {
        Address address = getAddress(instance);
        InternalPartition[] partitions = getPartitionService(instance).getInternalPartitions();
        for (InternalPartition partition : partitions) {
            for (int i = 0; i < InternalPartition.MAX_REPLICA_COUNT; i++) {
                assertNotEquals("Replica " + i + " of partition " + partition + " must not have the given address",
                        address, partition.getReplicaAddress(i));
            }
        }
    }

    private static void assertNoPartitionsAssignedEventually(HecatonInstance instance) {
        assertTrueEventually(() -> assertNoPartitionsAssigned(instance));
    }

    private static Member getMember(HecatonInstance hz) {
        return hz.getCluster().getLocalMember();
    }
}
