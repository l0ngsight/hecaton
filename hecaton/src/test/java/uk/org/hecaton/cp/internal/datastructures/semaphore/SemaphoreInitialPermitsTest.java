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
import uk.org.hecaton.cp.ISemaphore;
import uk.org.hecaton.cp.internal.HecatonRaftTestSupport;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SemaphoreInitialPermitsTest extends HecatonRaftTestSupport {

    @Parameters(name = "cpSubsystemEnabled:{0}, jdkCompatible:{1}, initialPermits:{2}")
    public static Collection<Object> parameters() {
        return asList(
                new Object[]{false, false, 0},
                new Object[]{false, false, 1},
                new Object[]{false, true, 0},
                new Object[]{false, true, 1},
                new Object[]{true, false, 0},
                new Object[]{true, false, 1},
                new Object[]{true, true, 0},
                new Object[]{true, true, 1});
    }

    @Parameter(0)
    public boolean cpSubsystemEnabled;

    @Parameter(1)
    public boolean jdkCompatible;

    @Parameter(2)
    public int initialPermits;

    private final String objectName = "semaphore1";

    private ISemaphore semaphore;

    @Before
    public void init() {
        factory = createHecatonInstanceFactory(3);
        HecatonInstance[] instances = newInstances(3, 3, 0);
        semaphore = instances[0].getCPSubsystem().getSemaphore(objectName);
    }

    @Test
    public void testSemaphoreAlreadyInitialized() {
        assumeTrue(initialPermits > 0);

        assertTrue(semaphore.availablePermits() > 0);
        assertFalse(semaphore.init(10));
    }

    @Test
    public void testSemaphoreInit() {
        assumeTrue(initialPermits == 0);

        assertEquals(0, semaphore.availablePermits());
        assertTrue(semaphore.init(10));
    }

    @Override
    protected Config createConfig(int cpNodeCount, int groupSize) {
        Config config = super.createConfig(cpNodeCount, groupSize);
        CPSubsystemConfig cpSubsystemConfig = config.getCPSubsystemConfig();

        if (cpSubsystemEnabled) {
            cpSubsystemConfig.setCPMemberCount(cpNodeCount).setGroupSize(groupSize);
        }

        SemaphoreConfig semaphoreConfig = new SemaphoreConfig(objectName, jdkCompatible, initialPermits);
        cpSubsystemConfig.addSemaphoreConfig(semaphoreConfig);
        return config;
    }

}
