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

package uk.org.hecaton.client.test;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.RoutingMode;
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

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HecatonFactoryTest extends HecatonTestSupport {

    private TestHecatonFactory instanceFactory;

    @Before
    public void setUp() {
        instanceFactory = new TestHecatonFactory();
    }

    @After
    public void tearDown() {
        instanceFactory.terminateAll();
    }

    @Test
    public void testTestHecatonInstanceFactory_allMembersRoutingClients() {
        final HecatonInstance instance1 = instanceFactory.newHecatonInstance();
        final HecatonInstance instance2 = instanceFactory.newHecatonInstance();
        final HecatonInstance instance3 = instanceFactory.newHecatonInstance();

        final HecatonInstance client1 = instanceFactory.newHecatonClient();
        final HecatonInstance client2 = instanceFactory.newHecatonClient();

        assertTrueEventually(() -> {
            touchRandomNode(client1);
            touchRandomNode(client2);

            assertClusterSize(3, instance1, instance2, instance3);

            assertEquals(2, instance1.getClientService().getConnectedClients().size());
            assertEquals(2, instance2.getClientService().getConnectedClients().size());
            assertEquals(2, instance3.getClientService().getConnectedClients().size());
        });
    }

    @Test
    public void testTestHecatonInstanceFactory_dummyClients() {
        final HecatonInstance instance1 = instanceFactory.newHecatonInstance();
        final HecatonInstance instance2 = instanceFactory.newHecatonInstance();
        final HecatonInstance instance3 = instanceFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER);
        final HecatonInstance client1 = instanceFactory.newHecatonClient(clientConfig);
        final HecatonInstance client2 = instanceFactory.newHecatonClient(clientConfig);

        assertClusterSizeEventually(3, instance1, instance2, instance3);

        assertTrueEventually(() -> {
            touchRandomNode(client1);
            touchRandomNode(client2);

            int actual = instance1.getClientService().getConnectedClients().size()
                    + instance2.getClientService().getConnectedClients().size()
                    + instance3.getClientService().getConnectedClients().size();
            assertEquals(2, actual);
        });

        assertClusterSizeEventually(3, client1, client2);
    }

    private static void touchRandomNode(HecatonInstance hecatonInstance) {
        hecatonInstance.getMap(randomString()).get(randomString());
    }
}
