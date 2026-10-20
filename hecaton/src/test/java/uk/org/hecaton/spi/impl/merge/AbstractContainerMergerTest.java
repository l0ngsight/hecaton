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

package uk.org.hecaton.spi.impl.merge;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.RequireAssertEnabled;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.spi.impl.merge.TestMergeOperation.OperationMode.BLOCKS;
import static uk.org.hecaton.spi.impl.merge.TestMergeOperation.OperationMode.THROWS_EXCEPTION;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AbstractContainerMergerTest extends HecatonTestSupport {

    private NodeEngineImpl nodeEngine;
    private TestContainerCollector collector;
    private TestContainerCollector emptyCollector;

    @Before
    public void setUp() {
        HecatonInstance hecatonInstance = createHecatonInstance();
        nodeEngine = getNodeEngineImpl(hecatonInstance);
        warmUpPartitions(hecatonInstance);

        collector = new TestContainerCollector(nodeEngine, true, true);
        collector.run();

        emptyCollector = new TestContainerCollector(nodeEngine, false, true);
        emptyCollector.run();
    }

    /**
     * Tests that the merger finished under normal conditions.
     */
    @Test
    @RequireAssertEnabled
    public void testMergerRun() {
        TestMergeOperation operation = new TestMergeOperation();
        TestContainerMerger merger = new TestContainerMerger(collector, nodeEngine, operation);

        merger.run();

        assertTrue("Expected the merge operation to be invoked", operation.hasBeenInvoked);
        assertTrue("Expected collected containers to be destroyed", collector.onDestroyHasBeenCalled);
    }

    /**
     * Tests that the merger finishes, even if the merge operation throws an exception.
     */
    @Test
    @RequireAssertEnabled
    public void testMergerRun_whenMergeOperationThrowsException_thenMergerFinishesNormally() {
        TestMergeOperation operation = new TestMergeOperation(THROWS_EXCEPTION);
        TestContainerMerger merger = new TestContainerMerger(collector, nodeEngine, operation);

        merger.run();

        assertTrue("Expected the merge operation to be invoked", operation.hasBeenInvoked);
        assertTrue("Expected collected containers to be destroyed", collector.onDestroyHasBeenCalled);
    }

    /**
     * Tests that the merger finishes eventually, when the merge operation blocks.
     */
    @Test
    @RequireAssertEnabled
    @Category(SlowTest.class)
    public void testMergerRun_whenMergeOperationBlocks_thenMergerFinishesEventually() {
        TestMergeOperation operation = new TestMergeOperation(BLOCKS);
        TestContainerMerger merger = new TestContainerMerger(collector, nodeEngine, operation);

        merger.run();
        operation.unblock();

        assertTrue("Expected the merge operation to be invoked", operation.hasBeenInvoked);
        assertTrue("Expected collected containers to be destroyed", collector.onDestroyHasBeenCalled);
    }

    /**
     * Tests that the merger finishes, when it's interrupted.
     */
    @Test
    @RequireAssertEnabled
    public void testMergerRun_whenMergerIsInterrupted_thenMergerFinishesEventually() {
        TestMergeOperation operation = new TestMergeOperation(BLOCKS);
        final TestContainerMerger merger = new TestContainerMerger(collector, nodeEngine, operation);

        Thread thread = new Thread(merger::run);
        thread.start();
        thread.interrupt();
        assertJoinable(thread);
        operation.unblock();

        // we cannot assert if the operation has been invoked, since the interruption could be faster
        assertTrue("Expected collected containers to be destroyed", collector.onDestroyHasBeenCalled);
    }

    /**
     * Tests that the merger finishes without invoking merge operations, if no containers have been collected.
     */
    @Test
    @RequireAssertEnabled
    public void testMergerRun_whenEmptyCollector_thenMergerDoesNotRun() {
        TestMergeOperation operation = new TestMergeOperation();
        TestContainerMerger merger = new TestContainerMerger(emptyCollector, nodeEngine, operation);

        merger.run();

        assertFalse("Expected the merge operation not to be invoked", operation.hasBeenInvoked);
        assertFalse("Expected collected containers not to be destroyed", collector.onDestroyHasBeenCalled);
    }

    /**
     * Tests that an assertion is triggered, if the merger implementation doesn't call
     * {@link AbstractContainerMerger#invoke(String, Operation, int)}, when there are
     * collected containers.
     */
    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testMergerRun_whenMissingOperationInvocation_thenMergerThrowsAssertion() {
        TestContainerMerger merger = new TestContainerMerger(collector, nodeEngine, null);

        merger.run();
    }
}
