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

import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.OutOfMemoryErrorDispatcher;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TestHecatonFactoryTest extends HecatonTestSupport {

    private HecatonInstance server;
    private final TestHecatonFactory factory = new TestHecatonFactory();

    @Before
    public void setUp() throws Exception {
        server = factory.newHecatonInstance();
    }

    @After
    public void tearDown() throws Exception {
        server.shutdown();
    }

    @Test
    public void no_leaking_client_after_shutdown() {
        HecatonInstance client = factory.newHecatonClient();
        HecatonClientInstanceImpl clientInstanceImpl = ((HecatonClientProxy) client).client;

        client.getLifecycleService().shutdown();

        assertNoLeakingClient(clientInstanceImpl);
    }

    @Test
    public void no_leaking_client_after_terminate() {
        HecatonInstance client = factory.newHecatonClient();
        HecatonClientInstanceImpl clientInstanceImpl = ((HecatonClientProxy) client).client;

        client.getLifecycleService().terminate();

        assertNoLeakingClient(clientInstanceImpl);
    }

    @Test
    public void no_leaking_client_after_shutdownAll() {
        HecatonInstance client1 = factory.newHecatonClient();
        HecatonInstance client2 = factory.newHecatonClient();
        HecatonClientInstanceImpl clientInstanceImpl1 = ((HecatonClientProxy) client1).client;
        HecatonClientInstanceImpl clientInstanceImpl2 = ((HecatonClientProxy) client2).client;

        factory.shutdownAll();

        assertNoLeakingClient(clientInstanceImpl1);
        assertNoLeakingClient(clientInstanceImpl2);
    }

    @Test
    public void no_leaking_client_after_terminateAll() {
        HecatonInstance client1 = factory.newHecatonClient();
        HecatonInstance client2 = factory.newHecatonClient();
        HecatonClientInstanceImpl clientInstanceImpl1 = ((HecatonClientProxy) client1).client;
        HecatonClientInstanceImpl clientInstanceImpl2 = ((HecatonClientProxy) client2).client;

        factory.terminateAll();

        assertNoLeakingClient(clientInstanceImpl1);
        assertNoLeakingClient(clientInstanceImpl2);
    }

    /**
     * Check if we removed all client object related
     * references from {@link OutOfMemoryErrorDispatcher}
     * and {@link HecatonClient} internal client registry.
     */
    private static void assertNoLeakingClient(HecatonClientInstanceImpl clientInstanceImpl) {
        String instanceName = clientInstanceImpl.getName();
        HecatonInstance[] registeredClients = OutOfMemoryErrorDispatcher.getClientInstancesRef().get();
        for (HecatonInstance registeredClient : registeredClients) {
            assertNotSame("We expect no registered ref for the clientInstanceImpl", clientInstanceImpl, registeredClient);
        }

        assertNull(HecatonClient.getHecatonClientByName(instanceName));
    }
}
