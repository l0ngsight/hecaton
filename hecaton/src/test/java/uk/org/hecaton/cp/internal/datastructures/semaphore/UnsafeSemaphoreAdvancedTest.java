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

package uk.org.hecaton.cp.internal.datastructures.semaphore;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.cp.CPSubsystemConfig;
import uk.org.hecaton.config.cp.SemaphoreConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.cp.internal.RaftOp;
import uk.org.hecaton.cp.internal.RaftService;
import uk.org.hecaton.cp.internal.operation.unsafe.UnsafeRaftReplicateOp;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnsafeSemaphoreAdvancedTest extends AbstractSemaphoreAdvancedTest {

    @Override
    protected String getProxyName() {
        String group = generateKeyOwnedBy(primaryInstance);
        return objectName + "@" + group;
    }

    @Override
    protected HecatonInstance[] createInstances() {
        return factory.newInstances(createConfig(), 2);
    }

    @Override
    protected HecatonInstance getPrimaryInstance() {
        return instances[0];
    }

    @Override
    protected HecatonInstance getProxyInstance() {
        return instances[1];
    }

    @Override
    protected HecatonInstance leaderInstanceOf(CPGroupId groupId) {
        return primaryInstance;
    }

    private Config createConfig() {
        Config config = new Config();
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();
        cpSubsystemConfig.setSessionTimeToLiveSeconds(10);
        cpSubsystemConfig.setSessionHeartbeatIntervalSeconds(1);

        SemaphoreConfig semaphoreConfig = new SemaphoreConfig();
        semaphoreConfig.setName(objectName);
        cpSubsystemConfig.addSemaphoreConfig(semaphoreConfig);
        return config;
    }

    @Override
    protected <T> InternalCompletableFuture<T> invokeRaftOp(RaftGroupId groupId, RaftOp op) {
        RaftService service = getNodeEngineImpl(proxyInstance).getService(RaftService.SERVICE_NAME);
        return service.getInvocationManager().invokeOnPartition(new UnsafeRaftReplicateOp(groupId, op));
    }
}
