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

package uk.org.hecaton.splitbrainprotection.durableexecutor;

import uk.org.hecaton.durableexecutor.DurableExecutorService;
import uk.org.hecaton.durableexecutor.StaleTaskIdException;
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DurableExecutorSplitBrainProtectionReadTest extends AbstractSplitBrainProtectionTest {

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
    public void isShutdown_splitBrainProtection() {
        exec(0).isShutdown();
    }

    @Test
    public void isShutdown_noSplitBrainProtection() {
        exec(3).isShutdown();
    }

    @Test
    public void isTerminated_splitBrainProtection() {
        exec(0).isTerminated();
    }

    @Test
    public void isTerminated_noSplitBrainProtection() {
        exec(3).isTerminated();
    }

    @Test
    public void awaitTermination_splitBrainProtection() throws Exception {
        exec(0).awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void awaitTermination_noSplitBrainProtection() throws Exception {
        exec(3).awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void retrieveResult_splitBrainProtection() throws Exception {
        try {
            exec(0).retrieveResult(123L).get();
        } catch (ExecutionException ex) {
            if (ex.getCause() instanceof StaleTaskIdException) {
                // expected & meaningless since not a real taskId
            }
        }
    }

    @Test
    public void retrieveResult_noSplitBrainProtection() {
        assertThatThrownBy(() -> exec(3).retrieveResult(125L).get())
                .hasCauseInstanceOf(SplitBrainProtectionException.class);
    }

    protected DurableExecutorService exec(int index) {
        return durableExec(index, splitBrainProtectionOn);
    }
}
