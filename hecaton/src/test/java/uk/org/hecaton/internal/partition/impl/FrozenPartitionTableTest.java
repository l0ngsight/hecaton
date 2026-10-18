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
import uk.org.hecaton.cluster.impl.MemberImpl;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.partition.PartitionReplica;
import uk.org.hecaton.internal.partition.PartitionTableView;
import uk.org.hecaton.spi.exception.TargetNotMemberException;
import uk.org.hecaton.spi.exception.WrongTargetException;
import uk.org.hecaton.spi.impl.operationservice.ExceptionAction;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.impl.OperationServiceImpl;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.OverridePropertyRule;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static uk.org.hecaton.instance.impl.TestUtil.terminateInstance;
import static uk.org.hecaton.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static uk.org.hecaton.internal.cluster.impl.ClusterJoinManager.STALE_JOIN_PREVENTION_DURATION_PROP;
import static uk.org.hecaton.test.Accessors.getClusterService;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.Accessors.getOperationService;
import static uk.org.hecaton.test.Accessors.getPartitionService;
import static uk.org.hecaton.test.OverridePropertyRule.clear;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FrozenPartitionTableTest extends HecatonTestSupport {

    @Rule
    public final OverridePropertyRule ruleStaleJoinPreventionDuration = clear(STALE_JOIN_PREVENTION_DURATION_PROP);

    @Test
    public void partitionTable_isFrozen_whenNodesLeave_duringClusterStateIsFrozen() {
        testPartitionTableIsFrozenDuring(ClusterState.FROZEN);
    }

    @Test
    public void partitionTable_isFrozen_whenNodesLeave_duringClusterStateIsPassive() {
        testPartitionTableIsFrozenDuring(ClusterState.PASSIVE);
    }

    private void testPartitionTableIsFrozenDuring(final ClusterState clusterState) {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        HecatonInstance[] instances = factory.newInstances();
        warmUpPartitions(instances);

        changeClusterStateEventually(instances[0], clusterState);
        List<HecatonInstance> instancesList = new ArrayList<>(asList(instances));
        Collections.shuffle(instancesList);

        final PartitionTableView partitionTable = getPartitionTable(instances[0]);

        while (instancesList.size() > 1) {
            final HecatonInstance instanceToShutdown = instancesList.remove(0);
            instanceToShutdown.shutdown();
            for (HecatonInstance instance : instancesList) {
                assertClusterSizeEventually(instancesList.size(), instance);
                assertEquals(partitionTable, getPartitionTable(instance));
            }
        }
    }

    @Test
    public void partitionTable_isFrozen_whenMemberReJoins_duringClusterStateIsFrozen() {
        partitionTable_isFrozen_whenMemberReJoins_duringClusterStateIs(ClusterState.FROZEN);
    }

    @Test
    public void partitionTable_isFrozen_whenMemberReJoins_duringClusterStateIsPassive() {
        partitionTable_isFrozen_whenMemberReJoins_duringClusterStateIs(ClusterState.PASSIVE);
    }

    private void partitionTable_isFrozen_whenMemberReJoins_duringClusterStateIs(ClusterState state) {
        Config config = new Config();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(4);
        HecatonInstance[] instances = factory.newInstances(config, 3);
        HecatonInstance hz1 = instances[0];
        HecatonInstance hz2 = instances[1];
        HecatonInstance hz3 = instances[2];
        Address hz3Address = getNode(hz3).getThisAddress();
        warmUpPartitions(instances);

        final PartitionTableView partitionTable = getPartitionTable(hz1);

        changeClusterStateEventually(hz2, state);

        final Member member3 = getClusterService(hz3).getLocalMember();

        terminateInstance(hz2);
        terminateInstance(hz3);

        HecatonInstance newInstance = factory.newHecatonInstance(hz3Address);
        hz3 = newInstance;
        Member newMember3 = getClusterService(hz3).getLocalMember();

        assertClusterSizeEventually(2, hz1, hz3);
        // ensure partition state is applied on the new member
        assertTrueEventually(() -> assertTrue(Accessors.isPartitionStateInitialized(newInstance)));

        final List<HecatonInstance> instanceList = asList(hz1, hz3);
        assertTrueAllTheTime(() -> {
            for (HecatonInstance instance : instanceList) {
                PartitionTableView newPartitionTable = getPartitionTable(instance);
                for (int i = 0; i < newPartitionTable.length(); i++) {
                    for (int j = 0; j < InternalPartition.MAX_REPLICA_COUNT; j++) {
                        PartitionReplica replica = partitionTable.getReplica(i, j);
                        PartitionReplica newReplica = newPartitionTable.getReplica(i, j);

                        if (replica == null) {
                            assertNull(newReplica);
                        } else if (replica.equals(PartitionReplica.from(member3))) {
                            assertEquals(PartitionReplica.from(newMember3), newReplica);
                        } else {
                            assertEquals(replica, newReplica);
                        }
                    }
                }
            }
        }, 5);
    }

    @Test
    public void partitionTable_shouldBeFixed_whenMemberLeaves_inFrozenState_thenStateChangesToActive() {
        testPartitionTableIsHealedWhenClusterStateIsActiveAfter(ClusterState.FROZEN);
    }

    @Test
    public void partitionTable_shouldBeFixed_whenMemberLeaves_inPassiveState_thenStateChangesToActive() {
        testPartitionTableIsHealedWhenClusterStateIsActiveAfter(ClusterState.PASSIVE);
    }

    private void testPartitionTableIsHealedWhenClusterStateIsActiveAfter(final ClusterState clusterState) {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        HecatonInstance[] instances = factory.newInstances();
        warmUpPartitions(instances);

        changeClusterStateEventually(instances[0], clusterState);

        List<HecatonInstance> instancesList = new ArrayList<>(asList(instances));
        Collections.shuffle(instancesList);
        final HecatonInstance instanceToShutdown = instancesList.remove(0);
        final Address addressToShutdown = getNode(instanceToShutdown).getThisAddress();
        instanceToShutdown.shutdown();

        for (HecatonInstance instance : instancesList) {
            assertClusterSizeEventually(2, instance);
        }

        changeClusterStateEventually(instancesList.get(0), ClusterState.ACTIVE);
        waitAllForSafeState(instancesList);

        for (HecatonInstance instance : instancesList) {
            PartitionTableView partitionTable = getPartitionTable(instance);
            for (int i = 0; i < partitionTable.length(); i++) {
                for (PartitionReplica replica : partitionTable.getReplicas(i)) {
                    if (replica == null) {
                        continue;
                    }
                    assertNotEquals(addressToShutdown, replica.address());
                }
            }
        }
    }

    @Test
    public void partitionTable_shouldBeFixed_whenMemberRestarts_usingNewUuid() {
        ruleStaleJoinPreventionDuration.setOrClearProperty("5");

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance hz1 = factory.newHecatonInstance();
        HecatonInstance hz2 = factory.newHecatonInstance();
        HecatonInstance hz3 = factory.newHecatonInstance();

        assertClusterSizeEventually(3, hz2, hz3);
        warmUpPartitions(hz1, hz2, hz3);

        changeClusterStateEventually(hz3, ClusterState.FROZEN);
        int member3PartitionId = getPartitionId(hz3);

        MemberImpl member3 = getNode(hz3).getLocalMember();
        hz3.shutdown();
        assertClusterSizeEventually(2, hz1, hz2);

        hz3 = factory.newHecatonInstance(member3.getAddress());
        assertClusterSizeEventually(3, hz1, hz2);
        waitAllForSafeState(hz1, hz2, hz3);

        OperationServiceImpl operationService = getOperationService(hz1);
        operationService.invokeOnPartition(null, new NonRetryablePartitionOperation(), member3PartitionId).join();
    }

    @Test
    public void partitionTable_shouldBeFixed_whenMemberRestarts_usingUuidOfAnotherMissingMember() {
        ruleStaleJoinPreventionDuration.setOrClearProperty("5");

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance hz1 = factory.newHecatonInstance();
        HecatonInstance hz2 = factory.newHecatonInstance();
        HecatonInstance hz3 = factory.newHecatonInstance();
        HecatonInstance hz4 = factory.newHecatonInstance();

        assertClusterSizeEventually(4, hz2, hz3);
        warmUpPartitions(hz1, hz2, hz3, hz4);

        changeClusterStateEventually(hz4, ClusterState.FROZEN);
        int member3PartitionId = getPartitionId(hz3);
        int member4PartitionId = getPartitionId(hz4);

        MemberImpl member3 = getNode(hz3).getLocalMember();
        MemberImpl member4 = getNode(hz4).getLocalMember();
        hz3.shutdown();
        hz4.shutdown();
        assertClusterSizeEventually(2, hz1, hz2);

        factory.builder().withAddress(member3.getAddress()).withUuid(member4.getUuid()).construct();
        assertClusterSizeEventually(3, hz1, hz2);
        waitAllForSafeState(hz1, hz2);

        OperationServiceImpl operationService = getOperationService(hz1);
        operationService.invokeOnPartition(null, new NonRetryablePartitionOperation(), member3PartitionId).join();

        try {
            operationService.invokeOnPartition(null, new NonRetryablePartitionOperation(), member4PartitionId).joinInternal();
            fail("Invocation to missing member should have failed!");
        } catch (TargetNotMemberException ignored) {
        }
    }

    private static PartitionTableView getPartitionTable(HecatonInstance instance) {
        return getPartitionService(instance).createPartitionTableView();
    }

    public static class NonRetryablePartitionOperation extends Operation {
        @Override
        public void run() throws Exception {
        }

        @Override
        public ExceptionAction onInvocationException(Throwable throwable) {
            if (throwable instanceof WrongTargetException || throwable instanceof TargetNotMemberException) {
                return ExceptionAction.THROW_EXCEPTION;
            }
            return super.onInvocationException(throwable);
        }
    }
}
