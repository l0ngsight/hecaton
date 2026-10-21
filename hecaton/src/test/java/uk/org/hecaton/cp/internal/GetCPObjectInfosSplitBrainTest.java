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

package uk.org.hecaton.cp.internal;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.CPObjectInfo;
import uk.org.hecaton.cp.exception.NotLeaderException;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static uk.org.hecaton.cp.CPSubsystem.ATOMIC_LONG;
import static uk.org.hecaton.cp.internal.HecatonRaftTestSupport.waitAllForLeaderElection;
import static uk.org.hecaton.cp.internal.HecatonRaftTestSupport.waitUntilCPDiscoveryCompleted;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAX_NO_HEARTBEAT_SECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class GetCPObjectInfosSplitBrainTest extends RaftSplitBrainTestSupport {

    @Override
    protected Config config() {
        Config config = super.config();
        config.getCPSubsystemConfig().getRaftAlgorithmConfig().setLeaderHeartbeatPeriodInMillis(SECONDS.toMillis(3));
        config.getCPSubsystemConfig().getRaftAlgorithmConfig().setMaxMissedLeaderHeartbeatCount(2);
        MAX_NO_HEARTBEAT_SECONDS.setSystemProperty("5");
        return config;
    }

    @Override
    protected void onBeforeSplitBrainCreated(HecatonInstance[] instances) {
        waitUntilCPDiscoveryCompleted(instances);

        instances[0].getCPSubsystem().getAtomicLong("long").set(1);
    }

    @Override
    protected void onAfterSplitBrainCreated(HecatonInstance[] firstBrain, HecatonInstance[] secondBrain) {
        sleepSeconds(10);
        CPGroupId groupId = groupId(firstBrain[0], "default");

        // firstBrain is the majority, the call should succeed there
        for (HecatonInstance instance : firstBrain) {
            Iterable<CPObjectInfo> infos = instance.getCPSubsystem().getObjectInfos(groupId, ATOMIC_LONG);
            assertThat(infos)
                    .extracting(CPObjectInfo::name, CPObjectInfo::serviceName, CPObjectInfo::groupId)
                    .containsExactly(tuple("long", ATOMIC_LONG, groupId));
        }

        // The call on minority should result in an exception
        for (HecatonInstance instance : secondBrain) {
            assertThatThrownBy(() ->
                    instance.getCPSubsystem().getObjectInfos(groupId, ATOMIC_LONG)
            ).isInstanceOf(NotLeaderException.class);
        }
    }

    @Override
    protected void onAfterSplitBrainHealed(HecatonInstance[] instances) throws Exception {
        CPGroupId groupId = groupId(instances[4], "default");
        waitAllForLeaderElection(instances, groupId);

        Iterable<CPObjectInfo> objectInfos = instances[4].getCPSubsystem().getObjectInfos(groupId, ATOMIC_LONG);
        assertThat(objectInfos)
                .extracting(CPObjectInfo::name, CPObjectInfo::serviceName, CPObjectInfo::groupId)
                .containsExactly(tuple("long", ATOMIC_LONG, groupId));
    }

    CPGroupId groupId(HecatonInstance instance, String name) {
        try {
            return instance.getCPSubsystem()
                    .getCPSubsystemManagementService()
                    .getCPGroup(name)
                    .toCompletableFuture()
                    .get().id();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
