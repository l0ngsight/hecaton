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
import uk.org.hecaton.core.MemberLeftException;
import uk.org.hecaton.internal.cluster.impl.operations.PromoteLiteMemberOp;
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

import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static uk.org.hecaton.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static uk.org.hecaton.internal.cluster.impl.ClusterDataSerializerHook.EXPLICIT_SUSPICION;
import static uk.org.hecaton.internal.cluster.impl.ClusterDataSerializerHook.F_ID;
import static uk.org.hecaton.internal.cluster.impl.ClusterDataSerializerHook.HEARTBEAT;
import static uk.org.hecaton.internal.cluster.impl.ClusterDataSerializerHook.MEMBER_INFO_UPDATE;
import static uk.org.hecaton.internal.cluster.impl.ClusterDataSerializerHook.PROMOTE_LITE_MEMBER;
import static uk.org.hecaton.internal.util.RootCauseMatcher.rootCause;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class, SerializationSamplesExcluded.class})
public class PromoteLiteMemberTest extends HecatonTestSupport {

    @Test
    public void liteMaster_promoted() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config().setLiteMember(true));
        HecatonInstance hz2 = factory.newHecatonInstance(new Config());
        HecatonInstance hz3 = factory.newHecatonInstance(new Config());

        hz1.getCluster().promoteLocalLiteMember();
        assertFalse(getMember(hz1).isLiteMember());
        assertAllNormalMembers(hz1.getCluster());

        assertAllNormalMembersEventually(hz2.getCluster());
        assertAllNormalMembersEventually(hz3.getCluster());
    }

    @Test
    public void liteMember_promoted() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config());
        HecatonInstance hz2 = factory.newHecatonInstance(new Config().setLiteMember(true));
        HecatonInstance hz3 = factory.newHecatonInstance(new Config());

        hz2.getCluster().promoteLocalLiteMember();
        assertFalse(getMember(hz2).isLiteMember());
        assertAllNormalMembers(hz1.getCluster());

        assertAllNormalMembersEventually(hz2.getCluster());
        assertAllNormalMembersEventually(hz3.getCluster());
    }

    @Test
    public void normalMember_promotion_shouldFail_onLocal() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config());
        factory.newHecatonInstance(new Config());

        assertThatThrownBy(() -> hz1.getCluster().promoteLocalLiteMember())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void normalMember_promotion_shouldFail_onNonMaster() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        factory.newHecatonInstance(new Config());
        HecatonInstance hz2 = factory.newHecatonInstance(new Config());
        HecatonInstance hz3 = factory.newHecatonInstance(new Config());

        PromoteLiteMemberOp op = new PromoteLiteMemberOp();
        op.setCallerUuid(getMember(hz2).getUuid());

        InternalCompletableFuture<MembersView> future =
                getOperationService(hz2).invokeOnTarget(ClusterServiceImpl.SERVICE_NAME, op, getAddress(hz3));
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .has(rootCause(IllegalStateException.class));
    }

    @Test
    public void normalMember_promotion_shouldBeNoop_onMaster() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config());
        HecatonInstance hz2 = factory.newHecatonInstance(new Config());

        PromoteLiteMemberOp op = new PromoteLiteMemberOp();
        op.setCallerUuid(getMember(hz2).getUuid());

        InternalCompletableFuture<MembersView> future =
                getOperationService(hz2).invokeOnTarget(ClusterServiceImpl.SERVICE_NAME, op, getAddress(hz1));
        future.join();
    }

    @Test
    public void notExistingMember_promotion_shouldFail() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config());
        HecatonInstance hz2 = factory.newHecatonInstance(new Config());

        PromoteLiteMemberOp op = new PromoteLiteMemberOp();
        op.setCallerUuid(UuidUtil.newUnsecureUUID());

        InternalCompletableFuture<MembersView> future =
                getOperationService(hz2).invokeOnTarget(ClusterServiceImpl.SERVICE_NAME, op, getAddress(hz1));
        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .has(rootCause(IllegalStateException.class));
    }

    @Test
    public void standaloneLiteMember_promoted() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz = factory.newHecatonInstance(new Config().setLiteMember(true));

        hz.getCluster().promoteLocalLiteMember();
        assertFalse(getMember(hz).isLiteMember());
        assertAllNormalMembers(hz.getCluster());

        warmUpPartitions(hz);
        assertPartitionsAssigned(hz);
    }

    @Test
    public void promotedMasterLiteMember_shouldHave_partitionsAssigned() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config().setLiteMember(true));
        HecatonInstance hz2 = factory.newHecatonInstance(new Config());
        HecatonInstance hz3 = factory.newHecatonInstance(new Config());

        warmUpPartitions(hz1, hz2, hz3);
        assertNoPartitionsAssigned(hz1);

        hz1.getCluster().promoteLocalLiteMember();

        assertPartitionsAssignedEventually(hz1);
        waitAllForSafeState(hz1, hz2, hz3);

        long partitionStamp = getPartitionService(hz1).getPartitionStateStamp();
        assertEquals(partitionStamp, getPartitionService(hz2).getPartitionStateStamp());
        assertEquals(partitionStamp, getPartitionService(hz3).getPartitionStateStamp());
    }

    @Test
    public void promotedLiteMember_shouldHave_partitionsAssigned() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config());
        HecatonInstance hz2 = factory.newHecatonInstance(new Config().setLiteMember(true));
        HecatonInstance hz3 = factory.newHecatonInstance(new Config());

        warmUpPartitions(hz1, hz2, hz3);
        assertNoPartitionsAssigned(hz2);

        hz2.getCluster().promoteLocalLiteMember();

        assertPartitionsAssignedEventually(hz2);

        waitAllForSafeState(hz1, hz2, hz3);

        long partitionStamp = getPartitionService(hz1).getPartitionStateStamp();
        assertEquals(partitionStamp, getPartitionService(hz2).getPartitionStateStamp());
        assertEquals(partitionStamp, getPartitionService(hz3).getPartitionStateStamp());
    }

    @Test
    public void promotion_shouldFail_whenClusterStatePassive() {
        promotion_shouldFail_whenClusterState_NotAllowMigration(ClusterState.PASSIVE);
    }

    @Test
    public void promotion_shouldFail_whenClusterStateFrozen() {
        promotion_shouldFail_whenClusterState_NotAllowMigration(ClusterState.FROZEN);
    }

    @Test
    public void promotion_shouldFail_whenClusterStateNoMigration() {
        promotion_shouldFail_whenClusterState_NotAllowMigration(ClusterState.NO_MIGRATION);
    }

    private void promotion_shouldFail_whenClusterState_NotAllowMigration(ClusterState state) {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config());
        HecatonInstance hz2 = factory.newHecatonInstance(new Config().setLiteMember(true));
        HecatonInstance hz3 = factory.newHecatonInstance(new Config());

        warmUpPartitions(hz1, hz2, hz3);
        changeClusterStateEventually(hz2, state);

        assertThatThrownBy(() -> hz2.getCluster().promoteLocalLiteMember())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void promotion_shouldFail_whenMastershipClaimInProgress_duringPromotion() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config());
        HecatonInstance hz2 = factory.newHecatonInstance(new Config().setLiteMember(true));

        // artificially set mastership claim flag
        ClusterServiceImpl clusterService = getNode(hz1).getClusterService();
        clusterService.getClusterJoinManager().setMastershipClaimInProgress();

        Cluster cluster = hz2.getCluster();
        assertThatThrownBy(cluster::promoteLocalLiteMember)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void promotion_shouldFail_whenMasterLeaves_duringPromotion() throws Exception {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config());
        HecatonInstance hz2 = factory.newHecatonInstance(new Config());
        HecatonInstance hz3 = factory.newHecatonInstance(new Config().setLiteMember(true));

        assertClusterSizeEventually(3, hz2);

        dropOperationsBetween(hz3, hz1, F_ID, singletonList(PROMOTE_LITE_MEMBER));
        Cluster cluster = hz3.getCluster();
        Future<Exception> future = spawn(() -> {
            try {
                cluster.promoteLocalLiteMember();
            } catch (Exception e) {
                return e;
            }
            return null;
        });
        assertPromotionInvocationStarted(hz3);

        hz1.getLifecycleService().terminate();
        assertClusterSizeEventually(2, hz2, hz3);

        Exception exception = future.get();
        // MemberLeftException is wrapped by HecatonException
        assertInstanceOf(MemberLeftException.class, exception.getCause());
    }

    @Test
    public void promotion_shouldFail_whenMasterIsSuspected_duringPromotion() throws Exception {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance hz1 = factory.newHecatonInstance(new Config());
        HecatonInstance hz2 = factory.newHecatonInstance(new Config());
        HecatonInstance hz3 = factory.newHecatonInstance(new Config().setLiteMember(true));

        assertClusterSizeEventually(3, hz2);

        rejectOperationsBetween(hz3, hz1, F_ID, asList(PROMOTE_LITE_MEMBER, EXPLICIT_SUSPICION));
        dropOperationsFrom(hz2, F_ID, asList(MEMBER_INFO_UPDATE, EXPLICIT_SUSPICION));
        dropOperationsFrom(hz1, F_ID, singletonList(HEARTBEAT));

        Cluster cluster = hz3.getCluster();
        Future<?> future = spawn(cluster::promoteLocalLiteMember);

        assertPromotionInvocationStarted(hz3);

        suspectMember(getNode(hz3), getNode(hz1));
        suspectMember(getNode(hz2), getNode(hz1));

        assertMasterAddressEventually(getAddress(hz2), hz3);

        dropOperationsBetween(hz3, hz1, F_ID, singletonList(EXPLICIT_SUSPICION));
        try {
            future.get();
            fail("Promotion should fail!");
        } catch (ExecutionException e) {
            assertInstanceOf(IllegalStateException.class, e.getCause());
        }
    }

    @Test
    public void test_lite_member_promotion_causes_no_data_loss_on_three_members() {
        int entryCount = 1000;

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        Config config = new Config().setLiteMember(true);

        // start first hecaton instance as a lite member
        HecatonInstance firstHecatonInstance = factory.newHecatonInstance(config);

        // start second and third hecaton instances as a lite member
        HecatonInstance secondHecatonInstance = factory.newHecatonInstance(config);
        HecatonInstance thirdHecatonInstance = factory.newHecatonInstance(config);

        // promote all instances to data members
        firstHecatonInstance.getCluster().promoteLocalLiteMember();
        secondHecatonInstance.getCluster().promoteLocalLiteMember();
        thirdHecatonInstance.getCluster().promoteLocalLiteMember();

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
        secondHecatonInstance.getLifecycleService().terminate();

        // backup count for the map is set to 1
        // even with 1 node down, no data loss is expected
        assertTrueEventually(() -> assertEquals(entryCount, firstHecatonInstance.getMap(mapName).size()));
        assertTrueEventually(() -> assertEquals(entryCount, thirdHecatonInstance.getMap(mapName).size()));
    }

    @Test
    public void test_lite_member_promotion_causes_no_data_loss_on_two_members() {
        int entryCount = 1000;

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        Config config = new Config().setLiteMember(true);

        // start first hecaton instance as a lite member
        HecatonInstance firstHecatonInstance = factory.newHecatonInstance(config);

        // start second hecaton instance as a lite member
        HecatonInstance secondHecatonInstance = factory.newHecatonInstance(config);

        // promote all instances to data members
        firstHecatonInstance.getCluster().promoteLocalLiteMember();

        secondHecatonInstance.getCluster().promoteLocalLiteMember();

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
        secondHecatonInstance.getLifecycleService().terminate();

        // backup count for the map is set to 1
        // even with 1 node down, no data loss is expected
        assertTrueEventually(() -> assertEquals(entryCount, firstHecatonInstance.getMap(mapName).size()));
    }

    private void assertPromotionInvocationStarted(HecatonInstance instance) {
        OperationServiceImpl operationService = getNode(instance).getNodeEngine().getOperationService();
        InvocationRegistry invocationRegistry = operationService.getInvocationRegistry();

        assertTrueEventually(() -> {
            for (Map.Entry<Long, Invocation> entry : invocationRegistry.entrySet()) {
                if (entry.getValue().op instanceof PromoteLiteMemberOp) {
                    return;
                }
            }
            fail("Cannot find PromoteLiteMemberOp invocation!");
        });
    }

    private static void assertPartitionsAssignedEventually(HecatonInstance instance) {
        assertTrueEventually(() -> assertPartitionsAssigned(instance));
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
                assertNotEquals(address, partition.getReplicaAddress(i));
            }
        }
    }

    private static void assertAllNormalMembers(Cluster cluster) {
        for (Member member : cluster.getMembers()) {
            assertFalse("Member is lite: " + member, member.isLiteMember());
        }
    }

    private static void assertAllNormalMembersEventually(Cluster cluster) {
        assertTrueEventually(() -> assertAllNormalMembers(cluster));
    }

    private static Member getMember(HecatonInstance hz) {
        return hz.getCluster().getLocalMember();
    }
}
