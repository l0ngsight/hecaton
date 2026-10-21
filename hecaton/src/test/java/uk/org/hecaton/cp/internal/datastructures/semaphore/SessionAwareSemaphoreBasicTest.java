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
import uk.org.hecaton.config.cp.SemaphoreConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.cp.internal.RaftOp;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.internal.util.RandomPicker;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SessionAwareSemaphoreBasicTest extends AbstractSessionAwareSemaphoreBasicTest {

    @Override
    protected String getProxyName() {
        return objectName + "@group";
    }

    @Override
    protected HecatonInstance getProxyInstance() {
        return instances[RandomPicker.getInt(instances.length)];
    }

    @Override
    protected HecatonInstance[] createInstances() {
        return newInstances(3);
    }

    @Override
    protected <T> InternalCompletableFuture<T> invokeRaftOp(RaftGroupId groupId, RaftOp raftOp) {
        return getRaftInvocationManager(instances[0]).invoke(groupId, raftOp);
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);

        SemaphoreConfig semaphoreConfig = new SemaphoreConfig();
        semaphoreConfig.setName(objectName);
        config.getCPSubsystemConfig().addSemaphoreConfig(semaphoreConfig);
        return config;
    }
}
