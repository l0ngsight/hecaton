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

package uk.org.hecaton.cp.internal.datastructures.atomiclong;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.IAtomicLong;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnsafeAtomicLongBasicTest extends AbstractAtomicLongBasicTest {

    private HecatonInstance primaryInstance;

    @Override
    protected HecatonInstance[] createInstances() {
        HecatonInstance[] instances = factory.newInstances(new Config(), 2);
        primaryInstance = instances[0];
        return instances;
    }

    @Override
    protected String getName() {
        warmUpPartitions(instances);
        assertNotNull(primaryInstance);
        String group = generateKeyOwnedBy(primaryInstance);
        return "long@" + group;
    }

    @Override
    protected IAtomicLong createAtomicLong(String name) {
        HecatonInstance instance = instances[1];
        return instance.getCPSubsystem().getAtomicLong(name);
    }

    @Test
    public void testPrimaryInstanceCrash() {
        atomicLong.set(111);
        waitAllForSafeState(instances);
        primaryInstance.getLifecycleService().terminate();
        assertEquals(111, atomicLong.get());
    }

    @Test
    public void testPrimaryInstanceShutdown() {
        atomicLong.set(111);
        primaryInstance.getLifecycleService().shutdown();
        assertEquals(111, atomicLong.get());
    }
}
