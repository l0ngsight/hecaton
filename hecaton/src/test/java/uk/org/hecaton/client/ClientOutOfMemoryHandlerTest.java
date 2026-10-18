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

package uk.org.hecaton.client;

import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientOutOfMemoryHandlerTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    private HecatonInstance client;
    private HecatonInstance[] instances;
    private ClientOutOfMemoryHandler outOfMemoryHandler;

    @Before
    public void setUp() {
        hecatonFactory.newHecatonInstance();
        client = hecatonFactory.newHecatonClient();

        instances = new HecatonInstance[2];
        instances[0] = ((HecatonClientProxy) client).client;
        instances[1] = null;

        outOfMemoryHandler = new ClientOutOfMemoryHandler();
    }

    @After
    public void cleanup() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testOnOutOfMemory() {
        outOfMemoryHandler.onOutOfMemory(new OutOfMemoryError(), instances);

        assertTrueEventually(() -> assertFalse("The client should be shutdown", client.getLifecycleService().isRunning()));
    }
}
