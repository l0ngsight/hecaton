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

package uk.org.hecaton.cp.internal.datastructures.countdownlatch;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.ICountDownLatch;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnsafeCountDownLatchAdvancedTest extends AbstractCountDownLatchAdvancedTest {

    private HecatonInstance primaryInstance;

    @Override
    protected String getName() {
        assertNotNull(primaryInstance);
        warmUpPartitions(instances);
        String group = generateKeyOwnedBy(primaryInstance);
        String objectName = "latch";
        return objectName + "@" + group;
    }

    @Override
    protected ICountDownLatch createLatch(String proxyName) {
        return instances[1].getCPSubsystem().getCountDownLatch(proxyName);
    }

    @Override
    protected HecatonInstance[] createInstances() {
        HecatonInstance[] instances = factory.newInstances(new Config(), 2);
        primaryInstance = instances[0];
        return instances;
    }

    @Override
    protected HecatonInstance leaderInstanceOf(CPGroupId groupId) {
        return primaryInstance;
    }
}
