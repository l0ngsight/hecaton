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

package uk.org.hecaton.instance.impl;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DefaultOutOfMemoryHandlerTest extends AbstractOutOfMemoryHandlerTest {

    private HecatonInstance[] instances;
    private DefaultOutOfMemoryHandler outOfMemoryHandler;

    @Before
    public void setUp() throws Exception {
        initHecatonInstances();

        instances = new HecatonInstance[2];
        instances[0] = hecatonInstance;
        instances[1] = null;

        outOfMemoryHandler = new DefaultOutOfMemoryHandler();
    }

    @Test
    public void testShouldHandle() {
        assertTrue(outOfMemoryHandler.shouldHandle(new OutOfMemoryError(DefaultOutOfMemoryHandler.GC_OVERHEAD_LIMIT_EXCEEDED)));
    }

    @Test
    public void testOnOutOfMemory() {
        outOfMemoryHandler.onOutOfMemory(new OutOfMemoryError(), instances);

        assertFalse("The member should be shutdown", hecatonInstance.getLifecycleService().isRunning());
    }
}
