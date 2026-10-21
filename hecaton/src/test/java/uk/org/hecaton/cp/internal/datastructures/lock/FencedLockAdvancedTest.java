/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.cp.internal.datastructures.lock;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.cp.CPSubsystemConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.internal.datastructures.spi.blocking.ResourceRegistry;
import uk.org.hecaton.cp.internal.raft.impl.RaftNodeImpl;
import uk.org.hecaton.cp.internal.raft.impl.log.LogEntry;
import uk.org.hecaton.cp.internal.raftop.snapshot.RestoreSnapshotOp;
import uk.org.hecaton.internal.util.RandomPicker;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static uk.org.hecaton.cp.internal.raft.impl.RaftUtil.getSnapshotEntry;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FencedLockAdvancedTest extends AbstractFencedLockAdvancedTest {

    private static final int LOG_ENTRY_COUNT_TO_SNAPSHOT = 10;

    private int groupSize = 3;

    @Override
    protected HecatonInstance getPrimaryInstance() {
        return instances[RandomPicker.getInt(instances.length)];
    }

    @Override
    protected String getProxyName() {
        return objectName + "@group";
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.getRaftAlgorithmConfig().setCommitIndexAdvanceCountToSnapshot(LOG_ENTRY_COUNT_TO_SNAPSHOT);
        cpSubsystemConfig.setSessionTimeToLiveSeconds(10);
        cpSubsystemConfig.setSessionHeartbeatIntervalSeconds(1);

        return config;
    }

    @Override
    protected HecatonInstance[] createInstances() {
        return newInstances(groupSize);
    }

    @Test
    public void testNewRaftGroupMemberSchedulesTimeoutsWithSnapshot() throws ExecutionException, InterruptedException {
        long fence = this.lock.lockAndGetFence();
        assertTrue(fence > 0);

        spawn(() -> {
            lock.tryLock(10, MINUTES);
        });

        CPGroupId groupId = this.lock.getGroupId();

        assertTrueEventually(() -> {
            HecatonInstance leader = getLeaderInstance(instances, groupId);
            LockService service = getNodeEngineImpl(leader).getService(LockService.SERVICE_NAME);
            ResourceRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertFalse(registry.getWaitTimeouts().isEmpty());
        });

        spawn(() -> {
            for (int i = 0; i < LOG_ENTRY_COUNT_TO_SNAPSHOT; i++) {
                lock.isLocked();
            }
        });

        assertTrueEventually(() -> {
            for (HecatonInstance instance : instances) {
                RaftNodeImpl raftNode = getRaftNode(instance, groupId);
                assertNotNull(raftNode);
                LogEntry snapshotEntry = getSnapshotEntry(raftNode);
                assertTrue(snapshotEntry.index() > 0);
                List<RestoreSnapshotOp> ops = (List<RestoreSnapshotOp>) snapshotEntry.operation();
                for (RestoreSnapshotOp op : ops) {
                    if (op.getServiceName().equals(LockService.SERVICE_NAME)) {
                        ResourceRegistry registry = (ResourceRegistry) op.getSnapshot();
                        assertFalse(registry.getWaitTimeouts().isEmpty());
                        return;
                    }
                }
                fail();
            }
        });

        HecatonInstance instanceToShutdown = (instances[0] == proxyInstance) ? instances[1] : instances[0];
        instanceToShutdown.shutdown();

        HecatonInstance newInstance = factory.newHecatonInstance(createConfig(groupSize, groupSize));
        newInstance.getCPSubsystem().getCPSubsystemManagementService().promoteToCPMember()
                   .toCompletableFuture().get();

        assertTrueEventually(() -> {
            RaftNodeImpl raftNode = getRaftNode(newInstance, groupId);
            assertNotNull(raftNode);
            assertTrue(getSnapshotEntry(raftNode).index() > 0);

            LockService service = getNodeEngineImpl(newInstance).getService(LockService.SERVICE_NAME);
            LockRegistry registry = service.getRegistryOrNull(groupId);
            assertNotNull(registry);
            assertFalse(registry.getWaitTimeouts().isEmpty());
            LockOwnershipState ownership = registry.getLockOwnershipState(objectName);
            assertTrue(ownership.isLocked());
            assertTrue(ownership.getLockCount() > 0);
            assertEquals(fence, ownership.getFence());
        });
    }

    @Override
    protected HecatonInstance leaderInstanceOf(CPGroupId groupId) {
        return getLeaderInstance(instances, groupId);
    }
}
