/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.cp.internal.persistence;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.CPMap;
import uk.org.hecaton.cp.IAtomicLong;
import uk.org.hecaton.cp.internal.CPMemberInfo;
import uk.org.hecaton.cp.internal.HecatonRaftTestSupport;
import uk.org.hecaton.cp.internal.datastructures.atomiclong.proxy.AtomicLongProxy;
import uk.org.hecaton.cp.internal.raft.impl.RaftNodeImpl;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static uk.org.hecaton.cp.internal.raft.impl.RaftUtil.getCommitIndex;
import static uk.org.hecaton.test.Accessors.getAddress;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * CP Subsystem persistence (F9): CP members restart with their CP identity and the state of their
 * CP groups, also at another address.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class CPPersistenceTest extends HecatonRaftTestSupport {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private File baseDir;

    @Test
    public void wholeClusterRestartKeepsTheCPData() throws IOException {
        baseDir = folder.newFolder();
        HecatonInstance[] instances = newInstances(3);
        IAtomicLong counter = instances[0].getCPSubsystem().getAtomicLong("counter");
        counter.set(41);
        counter.incrementAndGet();
        instances[1].getCPSubsystem().getAtomicLong("other@group1").set(7);
        CPMap<String, String> map = instances[2].getCPSubsystem().getMap("map");
        for (int i = 0; i < 300; i++) {
            // more commits than a snapshot holds
            map.put("key" + i, "value" + i);
        }
        UUID[] cpMembers = new UUID[instances.length];
        for (int i = 0; i < instances.length; i++) {
            cpMembers[i] = getRaftService(instances[i]).getLocalCPMember().getUuid();
        }
        for (HecatonInstance instance : instances) {
            instance.shutdown();
        }

        HecatonInstance[] restarted = new HecatonInstance[instances.length];
        for (int i = 0; i < restarted.length; i++) {
            restarted[i] = factory.newHecatonInstance(createConfig(3, 3));
        }
        assertClusterSizeEventually(3, restarted);
        waitUntilCPDiscoveryCompleted(restarted);
        for (HecatonInstance instance : restarted) {
            CPMemberInfo cpMember = getRaftService(instance).getLocalCPMember();
            assertThat(cpMember.getUuid()).isIn((Object[]) cpMembers);
            assertThat(cpMember.getAddress()).isEqualTo(getAddress(instance));
        }

        assertThat(restarted[0].getCPSubsystem().getAtomicLong("counter").get()).isEqualTo(42);
        assertThat(restarted[1].getCPSubsystem().getAtomicLong("other@group1").get()).isEqualTo(7);
        CPMap<String, String> restoredMap = restarted[2].getCPSubsystem().getMap("map");
        assertThat(restoredMap.get("key0")).isEqualTo("value0");
        assertThat(restoredMap.get("key299")).isEqualTo("value299");
        assertThat(restarted[0].getCPSubsystem().getAtomicLong("counter").incrementAndGet()).isEqualTo(43);
        assertTrueEventually(() -> assertThat(restarted[0].getCPSubsystem().getCPSubsystemManagementService()
                .getCPMembers().toCompletableFuture().get())
                .allMatch(member -> member.getAddress().equals(getAddress(restarted[0]))
                        || member.getAddress().equals(getAddress(restarted[1]))
                        || member.getAddress().equals(getAddress(restarted[2]))));
    }

    @Test
    public void crashedMemberRestartsAndRejoinsItsGroups() throws IOException {
        baseDir = folder.newFolder();
        HecatonInstance[] instances = newInstances(3);
        IAtomicLong counter = instances[0].getCPSubsystem().getAtomicLong("counter");
        counter.set(1);
        CPGroupId groupId = ((AtomicLongProxy) counter).getGroupId();
        UUID crashedCPMember = getRaftService(instances[2]).getLocalCPMember().getUuid();
        instances[2].getLifecycleService().terminate();
        counter.incrementAndGet();

        HecatonInstance restarted = factory.newHecatonInstance(createConfig(3, 3));
        assertThat(getRaftService(restarted).getLocalCPMember().getUuid()).isEqualTo(crashedCPMember);
        RaftNodeImpl leader = waitAllForLeaderElection(new HecatonInstance[]{instances[0], instances[1], restarted}, groupId);
        assertTrueEventually(() -> assertThat(getCommitIndex(getRaftNode(restarted, groupId)))
                .isEqualTo(getCommitIndex(leader)));

        // the restarted member is needed for a majority now
        instances[1].getLifecycleService().terminate();
        assertThat(counter.incrementAndGet()).isEqualTo(3);
        assertThat(restarted.getCPSubsystem().getAtomicLong("counter").get()).isEqualTo(3);
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        config.getCPSubsystemConfig().setPersistenceEnabled(true).setBaseDir(baseDir);
        return config;
    }
}
