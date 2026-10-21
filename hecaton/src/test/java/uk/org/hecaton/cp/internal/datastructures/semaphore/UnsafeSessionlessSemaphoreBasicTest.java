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
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.ISemaphore;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnsafeSessionlessSemaphoreBasicTest extends AbstractSessionlessSemaphoreBasicTest {

    protected HecatonInstance primaryInstance;
    protected String proxyName;

    @Override
    protected HecatonInstance[] createInstances() {
        Config config = new Config();
        SemaphoreConfig semaphoreConfig = new SemaphoreConfig(objectName, true, 0);
        config.getCPSubsystemConfig().addSemaphoreConfig(semaphoreConfig);
        return factory.newInstances(config, 2);
    }

    @Override
    protected ISemaphore createSemaphore() {
        primaryInstance = instances[0];
        String group = generateKeyOwnedBy(primaryInstance);
        proxyName = objectName + "@" + group;
        return createSemaphore(proxyName);
    }

    protected ISemaphore createSemaphore(String proxyName) {
        return instances[1].getCPSubsystem().getSemaphore(proxyName);
    }

    @Override
    protected HecatonInstance leaderInstanceOf(CPGroupId groupId) {
        return primaryInstance;
    }

    @Test
    public void testPrimaryInstanceCrash() {
        semaphore.init(11);
        waitAllForSafeState(instances);
        primaryInstance.getLifecycleService().terminate();
        assertEquals(11, semaphore.availablePermits());
    }

    @Test
    public void testPrimaryInstanceShutdown() {
        semaphore.init(11);
        primaryInstance.getLifecycleService().shutdown();
        assertEquals(11, semaphore.availablePermits());
    }
}
