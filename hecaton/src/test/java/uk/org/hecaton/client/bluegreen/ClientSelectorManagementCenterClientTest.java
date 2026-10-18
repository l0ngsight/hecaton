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

import uk.org.hecaton.client.config.ClientConfig;
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

import static uk.org.hecaton.client.impl.management.ManagementCenterService.MC_CLIENT_MODE_PROP;
import static uk.org.hecaton.test.Accessors.getClientEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientSelectorManagementCenterClientTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void cleanup() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testManagementCenterClient_doesNotGetDisconnected() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();

        final ClientEngineImpl clientEngineImpl = getClientEngineImpl(instance);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(MC_CLIENT_MODE_PROP.getName(), "true");
        hecatonFactory.newHecatonClient(clientConfig);

        clientEngineImpl.applySelector(ClientSelectors.none());

        assertTrueAllTheTime(() -> {
            Collection<ClientEndpoint> endpoints = clientEngineImpl.getEndpointManager().getEndpoints();
            assertEquals(1, endpoints.size());
        }, 10);
    }

}
