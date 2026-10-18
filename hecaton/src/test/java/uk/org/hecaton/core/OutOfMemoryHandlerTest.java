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

package uk.org.hecaton.core;

import uk.org.hecaton.instance.impl.AbstractOutOfMemoryHandlerTest;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OutOfMemoryHandlerTest extends AbstractOutOfMemoryHandlerTest {

    private TestOutOfMemoryHandler outOfMemoryHandler;

    @Before
    public void setUp() throws Exception {
        initHecatonInstances();

        outOfMemoryHandler = new TestOutOfMemoryHandler();
    }

    @Test
    public void testShouldHandle() {
        assertTrue(outOfMemoryHandler.shouldHandle(new OutOfMemoryError()));
    }

    @Test
    public void testTryCloseConnections() {
        outOfMemoryHandler.closeConnections(hecatonInstance);
    }

    @Test
    public void testTryCloseConnections_shouldDoNothingWithNullInstance() {
        outOfMemoryHandler.closeConnections(null);
    }

    @Test
    public void testTryCloseConnections_shouldDoNothingWhenThrowableIsThrown() {
        outOfMemoryHandler.closeConnections(hecatonInstanceThrowsException);
    }

    @Test
    public void testTryShutdown() {
        outOfMemoryHandler.shutdown(hecatonInstance);
    }

    @Test
    public void testTryShutdown_shouldDoNothingWithNullInstance() {
        outOfMemoryHandler.shutdown(null);
    }

    @Test
    public void testTryShutdown_shouldDoNothingWhenThrowableIsThrown() {
        outOfMemoryHandler.shutdown(hecatonInstanceThrowsException);
    }

    static class TestOutOfMemoryHandler extends OutOfMemoryHandler {

        void closeConnections(HecatonInstance hecatonInstance) {
            tryCloseConnections(hecatonInstance);
        }

        void shutdown(HecatonInstance hecatonInstance) {
            tryShutdown(hecatonInstance);
        }

        @Override
        public void onOutOfMemory(OutOfMemoryError oome, HecatonInstance[] hecatonInstances) {
        }
    }
}
