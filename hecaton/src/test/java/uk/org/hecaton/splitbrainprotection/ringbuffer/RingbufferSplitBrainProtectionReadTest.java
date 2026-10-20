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

package uk.org.hecaton.splitbrainprotection.ringbuffer;

import uk.org.hecaton.core.IFunction;
import uk.org.hecaton.ringbuffer.Ringbuffer;
import uk.org.hecaton.splitbrainprotection.AbstractSplitBrainProtectionTest;
import uk.org.hecaton.splitbrainprotection.SplitBrainProtectionException;
import uk.org.hecaton.splitbrainprotection.SplitBrainProtectionOn;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import static java.util.Arrays.asList;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RingbufferSplitBrainProtectionReadTest extends AbstractSplitBrainProtectionTest {

    @Parameters(name = "splitBrainProtectionType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{SplitBrainProtectionOn.READ}, {SplitBrainProtectionOn.READ_WRITE}});
    }

    @Parameter
    public static SplitBrainProtectionOn splitBrainProtectionOn;


    @BeforeClass
    public static void setUp() {
        initTestEnvironment(smallInstanceConfig(), new TestHecatonInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Test
    public void capacity_splitBrainProtection() {
        ring(0).capacity();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void capacity_noSplitBrainProtection() {
        ring(3).capacity();
    }

    @Test
    public void size_splitBrainProtection() {
        ring(0).size();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void size_noSplitBrainProtection() {
        ring(3).size();
    }

    @Test
    public void tailSequence_splitBrainProtection() {
        ring(0).tailSequence();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void tailSequence_noSplitBrainProtection() {
        ring(3).tailSequence();
    }

    @Test
    public void headSequence_splitBrainProtection() {
        ring(0).headSequence();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void headSequence_noSplitBrainProtection() {
        ring(3).headSequence();
    }

    @Test
    public void remainingCapacity_splitBrainProtection() {
        ring(0).remainingCapacity();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void remainingCapacity_noSplitBrainProtection() {
        ring(3).remainingCapacity();
    }

    @Test
    public void readOne_splitBrainProtection() throws InterruptedException {
        ring(0).readOne(1L);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void readOne_noSplitBrainProtection() throws InterruptedException {
        ring(3).readOne(1L);
    }

    @Test
    public void readManyAsync_splitBrainProtection() throws Exception {
        ring(0).readManyAsync(1L, 1, 1, new Filter()).toCompletableFuture().get();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void readManyAsync_noSplitBrainProtection() throws Throwable {
        try {
            ring(3).readManyAsync(1L, 1, 1, new Filter()).toCompletableFuture().get();
        } catch (Exception ex) {
            throw ex.getCause();
        }
    }

    private static class Filter implements IFunction<Object, Object> {
        @Override
        public Object apply(Object input) {
            return true;
        }
    }

    protected Ringbuffer ring(int index) {
        return ring(index, splitBrainProtectionOn);
    }
}
