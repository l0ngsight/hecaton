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

import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.instance.impl.HecatonNodeExtension;
import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.partition.PartitionReplica;
import uk.org.hecaton.internal.partition.PartitionTableView;
import uk.org.hecaton.internal.partition.ReadonlyInternalPartition;
import uk.org.hecaton.internal.util.RandomPicker;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getClusterService;
import static uk.org.hecaton.test.Accessors.getPartitionService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InternalPartitionServiceImplTest extends HecatonTestSupport {

    private HecatonInstance instance;
    private InternalPartitionServiceImpl partitionService;
    private Member localMember;
    private int partitionCount;
    private final AtomicBoolean startupDone = new AtomicBoolean(true);

    @Before
    public void setup() {
        instance = createHecatonInstanceFactory()
                .withNodeExtensionCustomizer(node -> new HecatonNodeExtension(node) {
                    @Override
                    public boolean isStartCompleted() {
                        return startupDone.get();
                    }
                })
                .newHecatonInstance();

        partitionService = (InternalPartitionServiceImpl) getPartitionService(instance);
        localMember = getClusterService(instance).getLocalMember();
        partitionCount = partitionService.getPartitionCount();
    }

    @Test(expected = HecatonInstanceNotActiveException.class)
    public void test_getPartitionOwnerOrWait_throwsException_afterNodeShutdown() {
        instance.shutdown();
        partitionService.getPartitionOwnerOrWait(0);
    }

    @Test
    public void test_initialAssignment() {
        partitionService.firstArrangement();

        int partitionCount = partitionService.getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            assertTrue(partitionService.isPartitionOwner(i));
        }
    }

    @Test
    public void test_initialAssignment_whenStartNotCompleted() {
        startupDone.set(false);

        partitionService.firstArrangement();

        assertFalse(partitionService.getPartitionStateManager().isInitialized());
        assertEquals(0, partitionService.getPartitionStateStamp());
        assertNull(partitionService.getPartitionOwner(0));
    }

    @Test
    public void test_initialAssignment_whenClusterNotActive() {
        instance.getCluster().changeClusterState(ClusterState.FROZEN);

        partitionService.firstArrangement();

        assertFalse(partitionService.getPartitionStateManager().isInitialized());
        assertEquals(0, partitionService.getPartitionStateStamp());
        assertNull(partitionService.getPartitionOwner(0));
    }

    @Test(expected = IllegalStateException.class)
    public void test_getPartitionOwnerOrWait_whenClusterNotActive() {
        instance.getCluster().changeClusterState(ClusterState.FROZEN);

        partitionService.firstArrangement();
        partitionService.getPartitionOwnerOrWait(0);
    }

    @Test
    public void test_setInitialState() {
        InternalPartition[] partitions = new InternalPartition[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            PartitionReplica[] replicas = new PartitionReplica[MAX_REPLICA_COUNT];
            replicas[0] = PartitionReplica.from(localMember);
            partitions[i] = new ReadonlyInternalPartition(replicas, i, RandomPicker.getInt(1, 10));
        }

        partitionService.setInitialState(new PartitionTableView(partitions));
        for (int i = 0; i < partitionCount; i++) {
            assertTrue(partitionService.isPartitionOwner(i));
        }
        assertNotEquals(0, partitionService.getPartitionStateStamp());
    }

    @Test(expected = IllegalStateException.class)
    public void test_setInitialState_multipleTimes() {
        InternalPartition[] partitions = new InternalPartition[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            PartitionReplica[] replicas = new PartitionReplica[MAX_REPLICA_COUNT];
            replicas[0] = PartitionReplica.from(localMember);
            partitions[i] = new ReadonlyInternalPartition(replicas, i, RandomPicker.getInt(1, 10));
        }

        partitionService.setInitialState(new PartitionTableView(partitions));
        partitionService.setInitialState(new PartitionTableView(partitions));
    }

    @Test
    public void test_getMemberPartitions_whenNotInitialized() {
        List<Integer> partitions = partitionService.getMemberPartitions(getAddress(instance));
        assertTrue(partitionService.getPartitionStateManager().isInitialized());
        assertEquals(partitionCount, partitions.size());
    }

    @Test
    public void test_getMemberPartitions_whenInitialized() {
        partitionService.firstArrangement();
        List<Integer> partitions = partitionService.getMemberPartitions(getAddress(instance));
        assertEquals(partitionCount, partitions.size());
    }

    @Test
    public void test_getMemberPartitionsIfAssigned_whenNotInitialized() {
        List<Integer> partitions = partitionService.getMemberPartitionsIfAssigned(getAddress(instance));
        assertThat(partitions).isEmpty();
    }

    @Test
    public void test_getMemberPartitionsIfAssigned_whenInitialized() {
        partitionService.firstArrangement();
        List<Integer> partitions = partitionService.getMemberPartitionsIfAssigned(getAddress(instance));
        assertEquals(partitionCount, partitions.size());
    }
}
