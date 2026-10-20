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
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.IPartition;
import uk.org.hecaton.internal.util.collection.PartitionIdSet;
import uk.org.hecaton.partition.NoDataMemberInClusterException;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getNode;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InternalPartitionServiceLiteMemberTest extends HecatonTestSupport {

    private final Config liteMemberConfig = new Config().setLiteMember(true);

    /**
     * PARTITION ASSIGNMENT
     **/

    @Test
    public void test_partitionsNotAssigned_withLiteMember() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(1);
        final HecatonInstance instance = factory.newHecatonInstance(liteMemberConfig);

        final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(instance);
        partitionService.firstArrangement();

        for (int i = 0; i < partitionService.getPartitionCount(); i++) {
            assertNull(partitionService.getPartition(i).getOwnerOrNull());
        }
    }

    @Test
    public void test_partitionsAreAssigned_afterDataMemberJoins() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance liteInstance = factory.newHecatonInstance(liteMemberConfig);

        final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(liteInstance);
        partitionService.firstArrangement();

        final HecatonInstance dataInstance = factory.newHecatonInstance();
        warmUpPartitions(liteInstance, dataInstance);

        for (int i = 0; i < partitionService.getPartitionCount(); i++) {
            assertEquals(getNode(dataInstance).getThisAddress(), partitionService.getPartition(i).getOwnerOrNull());
        }
    }

    /**
     * GET PARTITION
     **/

    @Test
    public void test_getPartition_nullPartitionOwnerOnMasterLiteMember() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(1);
        final HecatonInstance instance = factory.newHecatonInstance(liteMemberConfig);

        final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(instance);
        assertNull(partitionService.getPartition(0).getOwnerOrNull());
    }

    @Test
    public void test_getPartition_nullPartitionOwnerOnNonMasterLiteMember() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance master = factory.newHecatonInstance(liteMemberConfig);
        final HecatonInstance other = factory.newHecatonInstance(liteMemberConfig);

        assertClusterSize(2, master, other);

        for (HecatonInstance instance : asList(master, other)) {
            final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(instance);

            for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
                assertNull(partitionService.getPartition(0).getOwnerOrNull());
            }
        }
    }

    @Test
    public void test_getPartition_afterLiteMemberLeavesTheCluster() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance dataInstance = factory.newHecatonInstance();
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);

        warmUpPartitions(dataInstance, lite);

        lite.getLifecycleService().shutdown();

        assertClusterSizeEventually(1, dataInstance);

        final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(dataInstance);

        for (int i = 0; i < partitionService.getPartitionCount(); i++) {
            assertEquals(getNode(dataInstance).getThisAddress(), partitionService.getPartition(i).getOwnerOrNull());
        }
    }

    @Test
    public void test_getPartition_afterDataMemberLeavesTheCluster() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        final HecatonInstance master = factory.newHecatonInstance();
        final HecatonInstance dataInstance = factory.newHecatonInstance();
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);

        warmUpPartitions(lite);

        dataInstance.getLifecycleService().shutdown();

        for (HecatonInstance instance : asList(master, lite)) {
            final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(instance);

            assertTrueEventually(() -> {
                for (int i = 0; i < partitionService.getPartitionCount(); i++) {
                    assertEquals(getNode(master).getThisAddress(), partitionService.getPartition(i).getOwnerOrNull());
                }
            });
        }
    }

    /**
     * GET PARTITION ID SET
     */
    @Test
    public void test_getPartitionIdSet_fromKeyObjects() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        final HecatonInstance instance = factory.newHecatonInstance(liteMemberConfig);

        final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(instance);

        Set<Integer> partitionIds = Arrays.stream(partitionService.getPartitions()).map(IPartition::getPartitionId).collect(Collectors.toSet());

        Set<Object> keys = partitionIds.stream().map(id -> {
            String item;
            while (partitionService.getPartitionId((item = UUID.randomUUID().toString())) != id) { }
            return item;
        }).collect(Collectors.toSet());

        PartitionIdSet expected = new PartitionIdSet(partitionIds.size(), partitionIds);
        PartitionIdSet idSet = partitionService.getPartitionIdSet(keys);

        assertEquals(expected, idSet);
    }

    /**
     * GET PARTITION OWNER
     **/

    @Test
    public void test_getPartitionOwner_onMasterLiteMember() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(1);
        final HecatonInstance instance = factory.newHecatonInstance(liteMemberConfig);

        final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(instance);
        assertNull(partitionService.getPartitionOwner(0));
    }

    @Test
    public void test_getPartitionOwner_onNonMasterLiteMember() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance master = factory.newHecatonInstance(liteMemberConfig);
        final HecatonInstance other = factory.newHecatonInstance(liteMemberConfig);

        assertClusterSize(2, master, other);

        for (HecatonInstance instance : asList(master, other)) {
            final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(instance);
            assertNull(partitionService.getPartitionOwner(0));
        }
    }

    @Test(expected = NoDataMemberInClusterException.class)
    public void test_getPartitionOwnerOrWait_onMasterLiteMember() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(1);
        final HecatonInstance instance = factory.newHecatonInstance(liteMemberConfig);

        final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(instance);
        partitionService.getPartitionOwnerOrWait(0);
    }

    @Test(expected = NoDataMemberInClusterException.class)
    public void test_getPartitionOwnerOrWait_onNonMasterLiteMember() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance master = factory.newHecatonInstance(liteMemberConfig);
        final HecatonInstance other = factory.newHecatonInstance(liteMemberConfig);

        assertClusterSize(2, master, other);

        final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(other);
        partitionService.getPartitionOwnerOrWait(0);
    }

    @Test
    public void test_getPartitionOwnerOrWait_onLiteMemberAfterDataMemberTerminates() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance master = factory.newHecatonInstance();
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);

        warmUpPartitions(master, lite);

        master.getLifecycleService().terminate();

        assertTrueEventually(() -> {
            try {
                final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(lite);
                partitionService.getPartitionOwnerOrWait(0);
                fail();
            } catch (NoDataMemberInClusterException expected) {
                ignore(expected);
            }
        });
    }

    @Test
    public void test_getPartitionOwnerOrWait_onLiteMemberAfterDataMemberShutsDown() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance master = factory.newHecatonInstance();
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);

        assertClusterSize(2, master, lite);
        warmUpPartitions(master, lite);

        master.getLifecycleService().shutdown();

        assertTrueEventually(() -> {
            try {
                final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(lite);
                partitionService.getPartitionOwnerOrWait(0);
                fail();
            } catch (NoDataMemberInClusterException expected) {
                ignore(expected);
            }
        });
    }

    /**
     * GRACEFUL SHUTDOWN
     **/

    @Test(timeout = 120000)
    public void test_liteMemberCanShutdownSafely_withClusterSize1() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);
        lite.getLifecycleService().shutdown();
    }

    @Test(timeout = 120000)
    public void test_liteMemberCanShutdownSafely_withClusterSize2() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance lite1 = factory.newHecatonInstance(liteMemberConfig);
        factory.newHecatonInstance(liteMemberConfig);
        lite1.getLifecycleService().shutdown();
    }

    @Test(timeout = 120000)
    public void test_liteMemberCanShutdownSafely_whenDataMemberExistsInCluster() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);
        final HecatonInstance other = factory.newHecatonInstance();

        warmUpPartitions(lite, other);

        lite.getLifecycleService().shutdown();
    }

    @Test(timeout = 120000)
    public void test_dataMemberCanShutdownSafely_withClusterSize1() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(1);
        final HecatonInstance master = factory.newHecatonInstance();

        master.getLifecycleService().shutdown();
    }

    @Test(timeout = 120000)
    public void test_dataMemberCanShutdownSafely_whenOnlyLiteMemberExistsInCluster() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance master = factory.newHecatonInstance();
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);

        warmUpPartitions(master, lite);

        master.getLifecycleService().shutdown();
    }

    /**
     * TERMINATE
     **/

    @Test(timeout = 120000)
    public void test_liteMemberCanTerminate_withClusterSize1() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);
        lite.getLifecycleService().terminate();
    }

    @Test(timeout = 120000)
    public void test_liteMemberCanTerminate_withClusterSize2() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance lite1 = factory.newHecatonInstance(liteMemberConfig);
        final HecatonInstance lite2 = factory.newHecatonInstance(liteMemberConfig);

        assertClusterSize(2, lite1, lite2);

        lite1.getLifecycleService().terminate();
    }

    @Test(timeout = 120000)
    public void test_liteMemberCanTerminate_whenDataMemberExistsInCluster() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);
        final HecatonInstance other = factory.newHecatonInstance();

        warmUpPartitions(lite, other);

        lite.getLifecycleService().terminate();
    }

    @Test(timeout = 120000)
    public void test_dataMemberCanTerminate_withClusterSize1() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(1);
        final HecatonInstance master = factory.newHecatonInstance();

        master.getLifecycleService().terminate();
    }

    @Test(timeout = 120000)
    public void test_dataMemberCanTerminate_whenOnlyLiteMemberExistsInCluster() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance master = factory.newHecatonInstance();
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);

        warmUpPartitions(master, lite);

        master.getLifecycleService().terminate();
    }

    /**
     * GET MEMBER PARTITIONS MAP
     **/

    @Test(expected = NoDataMemberInClusterException.class)
    public void test_getMemberPartitionsMap_withOnlyLiteMembers() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(1);
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);

        final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(lite);
        partitionService.getMemberPartitionsMap();
    }

    @Test
    public void test_getMemberPartitionsMap_withLiteAndDataMembers() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);
        final HecatonInstance dataInstance = factory.newHecatonInstance();

        warmUpPartitions(lite, dataInstance);

        final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(lite);
        final Map<Address, List<Integer>> partitionsMap = partitionService.getMemberPartitionsMap();

        assertEquals(1, partitionsMap.size());
        final List<Integer> partitions = partitionsMap.get(getAddress(dataInstance));
        assertNotNull(partitions);
        assertFalse(partitions.isEmpty());
    }

    /**
     * MEMBER GROUP SIZE
     **/

    @Test
    public void test_memberGroupSize_withSingleLiteMember() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(1);
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);

        assertMemberGroupsSizeEventually(lite, 0);
    }

    @Test
    public void test_memberGroupSize_withMultipleLiteMembers() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);
        final HecatonInstance lite2 = factory.newHecatonInstance(liteMemberConfig);

        assertClusterSize(2, lite, lite2);

        for (HecatonInstance instance : asList(lite, lite2)) {
            assertMemberGroupsSizeEventually(instance, 0);
        }
    }

    @Test
    public void test_memberGroupSize_withOneLiteMemberAndOneDataMember() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);
        final HecatonInstance other = factory.newHecatonInstance();

        assertClusterSize(2, lite, other);

        for (HecatonInstance instance : asList(lite, other)) {
            assertMemberGroupsSizeEventually(instance, 1);
        }
    }

    @Test
    public void test_memberGroupSize_afterDataMemberLeavesTheCluster() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);
        final HecatonInstance other = factory.newHecatonInstance();

        assertClusterSize(2, lite, other);

        for (HecatonInstance instance : asList(lite, other)) {
            assertMemberGroupsSizeEventually(instance, 1);
        }

        other.getLifecycleService().shutdown();
        assertClusterSizeEventually(1, lite);
        assertMemberGroupsSizeEventually(lite, 0);
    }

    @Test
    public void test_memberGroupSize_afterLiteMemberLeavesTheCluster() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);
        final HecatonInstance other = factory.newHecatonInstance();

        assertClusterSize(2, lite, other);

        for (HecatonInstance instance : asList(lite, other)) {
            assertMemberGroupsSizeEventually(instance, 1);
        }

        lite.getLifecycleService().shutdown();
        assertClusterSizeEventually(1, other);
        assertMemberGroupsSizeEventually(other, 1);
    }

    private void assertMemberGroupsSizeEventually(final HecatonInstance instance, final int memberGroupSize) {
        assertTrueEventually(() -> {
            final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(instance);
            assertEquals(memberGroupSize, partitionService.getMemberGroupsSize());
        });
    }

    /**
     * MAX BACKUP COUNT
     **/

    @Test
    public void test_maxBackupCount_withSingleLiteMember() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(1);
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);

        assertMaxBackupCountEventually(lite, 0);
    }

    @Test
    public void test_maxBackupCount_withTwoLiteMembers() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);
        final HecatonInstance lite2 = factory.newHecatonInstance(liteMemberConfig);

        assertClusterSize(2, lite, lite2);

        for (HecatonInstance instance : asList(lite, lite2)) {
            assertMaxBackupCountEventually(instance, 0);
        }
    }

    @Test
    public void test_maxBackupCount_withOneLiteMemberAndOneDataMember() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);
        final HecatonInstance other = factory.newHecatonInstance();

        assertClusterSize(2, lite, other);

        for (HecatonInstance instance : asList(lite, other)) {
            assertMaxBackupCountEventually(instance, 0);
        }
    }

    @Test
    public void test_maxBackupCount_withOneLiteMemberAndTwoDataMembers() {
        final TestHecatonInstanceFactory factory = createHecatonInstanceFactory(3);
        final HecatonInstance lite = factory.newHecatonInstance(liteMemberConfig);
        final HecatonInstance other = factory.newHecatonInstance();
        final HecatonInstance other2 = factory.newHecatonInstance();

        assertClusterSize(3, lite, other2);
        assertClusterSizeEventually(3, other);

        for (HecatonInstance instance : asList(lite, other, other2)) {
            assertMaxBackupCountEventually(instance, 1);
        }
    }

    private void assertMaxBackupCountEventually(final HecatonInstance instance, final int maxBackupCount) {
        assertTrueEventually(() -> {
            final InternalPartitionServiceImpl partitionService = getInternalPartitionServiceImpl(instance);
            assertEquals(maxBackupCount, partitionService.getMaxAllowedBackupCount());
        });
    }

    private InternalPartitionServiceImpl getInternalPartitionServiceImpl(HecatonInstance instance) {
        return (InternalPartitionServiceImpl) getNode(instance).getPartitionService();
    }
}
