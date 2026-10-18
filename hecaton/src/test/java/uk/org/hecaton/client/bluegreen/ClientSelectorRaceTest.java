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

package uk.org.hecaton.client.bluegreen;

import uk.org.hecaton.client.impl.ClientEndpoint;
import uk.org.hecaton.client.impl.ClientEngineImpl;
import uk.org.hecaton.client.impl.ClientSelectors;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.LinkedList;

import static uk.org.hecaton.test.Accessors.getClientEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientSelectorRaceTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void cleanup() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testConcurrency() throws InterruptedException {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();

        final ClientEngineImpl clientEngineImpl = getClientEngineImpl(instance);

        LinkedList<Thread> threads = new LinkedList<>();
        int numberOfClients = 100;
        for (int i = 0; i < numberOfClients; i++) {
            Thread thread = new Thread(hecatonFactory::newHecatonClient);
            thread.start();
            threads.add(thread);

        }

        clientEngineImpl.applySelector(ClientSelectors.none());

        for (Thread thread : threads) {
            thread.join();
        }

        assertTrueEventually(() -> {
            Collection<ClientEndpoint> endpoints = clientEngineImpl.getEndpointManager().getEndpoints();
            assertEquals(0, endpoints.size());
        });

    }

}
