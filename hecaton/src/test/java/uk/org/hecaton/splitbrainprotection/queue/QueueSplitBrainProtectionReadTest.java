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

package uk.org.hecaton.splitbrainprotection.queue;

import uk.org.hecaton.collection.IQueue;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
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
public class QueueSplitBrainProtectionReadTest extends AbstractSplitBrainProtectionTest {

    private static final ILogger LOGGER = Logger.getLogger(QueueSplitBrainProtectionReadTest.class);

    @Parameters(name = "splitBrainProtectionType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][] {{SplitBrainProtectionOn.READ}, {SplitBrainProtectionOn.READ_WRITE}});
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
    public void element_splitBrainProtection() {
        LOGGER.info(String.valueOf(queue(0).size()));
        LOGGER.info(queue(0).getPartitionKey());
        queue(0).element();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void element_noSplitBrainProtection() {
        queue(3).element();
    }

    @Test
    public void peek_splitBrainProtection() {
        queue(0).peek();
    }

    @Test(expected = SplitBrainProtectionException.class)
    public void peek_noSplitBrainProtection() {
        queue(3).peek();
    }

    @Test
    public void getLocalQueueStats_splitBrainProtection() {
        try {
            queue(0).getLocalQueueStats();
        } catch (UnsupportedOperationException ex) {
            // client workaround
        }
    }

    @Test
    public void getLocalQueueStats_noSplitBrainProtection() {
        try {
            queue(3).getLocalQueueStats();
        } catch (UnsupportedOperationException ex) {
            // client workaround
        }
    }

    protected IQueue queue(int index) {
        return queue(index, splitBrainProtectionOn);
    }
}
