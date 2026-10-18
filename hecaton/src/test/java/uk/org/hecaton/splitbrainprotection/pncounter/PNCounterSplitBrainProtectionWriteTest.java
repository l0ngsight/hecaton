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

package uk.org.hecaton.splitbrainprotection.pncounter;

import uk.org.hecaton.crdt.pncounter.PNCounter;
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
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import static uk.org.hecaton.splitbrainprotection.SplitBrainProtectionOn.READ_WRITE;
import static uk.org.hecaton.splitbrainprotection.SplitBrainProtectionOn.WRITE;
import static java.util.Arrays.asList;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PNCounterSplitBrainProtectionWriteTest extends AbstractSplitBrainProtectionTest {

    @Parameterized.Parameter
    public static SplitBrainProtectionOn splitBrainProtectionOn;

    @Parameterized.Parameters(name = "splitBrainProtectionType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{{WRITE}, {READ_WRITE}});
    }

    @BeforeClass
    public static void setUp() {
        initTestEnvironment(smallInstanceConfig(), new TestHecatonInstanceFactory());
    }

    @AfterClass
    public static void tearDown() {
        shutdownTestEnvironment();
    }

    @Test
    public void addAndGet_splitBrainProtection() {
        pnCounter(0).addAndGet(1);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void addAndGet_noSplitBrainProtection() {
        pnCounter(3).addAndGet(1);
    }

    @Test
    public void getAndAdd_splitBrainProtection() {
        pnCounter(0).getAndAdd(1);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void getAndAdd_noSplitBrainProtection() {
        pnCounter(3).getAndAdd(1);
    }

    @Test
    public void incrementAndGet_splitBrainProtection() {
        pnCounter(0).incrementAndGet();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void incrementAndGet_noSplitBrainProtection() {
        pnCounter(3).incrementAndGet();
    }

    @Test
    public void getAndIncrement_splitBrainProtection() {
        pnCounter(0).getAndIncrement();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void getAndIncrement_noSplitBrainProtection() {
        pnCounter(3).getAndIncrement();
    }

    @Test
    public void subtractAndGet_splitBrainProtection() {
        pnCounter(0).subtractAndGet(1);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void subtractAndGet_noSplitBrainProtection() {
        pnCounter(3).subtractAndGet(1);
    }

    @Test
    public void getAndSubtract_splitBrainProtection() {
        pnCounter(0).getAndSubtract(1);
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void getAndSubtract_noSplitBrainProtection() {
        pnCounter(3).getAndSubtract(1);
    }

    @Test
    public void decrementAndGet_splitBrainProtection() {
        pnCounter(0).decrementAndGet();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void decrementAndGet_noSplitBrainProtection() {
        pnCounter(3).decrementAndGet();
    }

    @Test
    public void getAndDecrement_splitBrainProtection() {
        pnCounter(0).getAndDecrement();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void getAndDecrement_noSplitBrainProtection() {
        pnCounter(3).getAndDecrement();
    }

    protected PNCounter pnCounter(int index) {
        return pnCounter(index, splitBrainProtectionOn);
    }

}
