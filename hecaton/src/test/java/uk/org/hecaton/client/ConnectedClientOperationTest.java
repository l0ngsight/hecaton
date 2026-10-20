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

import uk.org.hecaton.client.impl.ClientEngineImpl;
import uk.org.hecaton.client.impl.operations.GetConnectedClientsOperation;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.nio.ConnectionType;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.Future;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConnectedClientOperationTest extends HecatonTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testNumberOfConnectedClients() {
        HecatonInstance h1 = factory.newHecatonInstance();
        HecatonInstance h2 = factory.newHecatonInstance();
        assertClusterSize(2, h1, h2);

        int numberOfClients = 6;
        for (int i = 0; i < numberOfClients; i++) {
            factory.newHecatonClient();
        }

        Node node = getNode(h1);
        Map<String, Long> clientStats = node.clientEngine.getActiveClientsInCluster();

        assertEquals(numberOfClients, clientStats.get(ConnectionType.JAVA_CLIENT).intValue());
        assertNull(clientStats.get(ConnectionType.CPP_CLIENT));
        assertNull(clientStats.get(ConnectionType.CSHARP_CLIENT));
        assertNull(clientStats.get(ConnectionType.NODEJS_CLIENT));
        assertNull(clientStats.get(ConnectionType.PYTHON_CLIENT));
        assertNull(clientStats.get(ConnectionType.GO_CLIENT));
    }

    @Test
    public void testGetConnectedClientsOperation_WhenZeroClientConnects() throws Exception {
        HecatonInstance instance = factory.newHecatonInstance();
        Node node = getNode(instance);

        Operation operation = new GetConnectedClientsOperation();
        OperationService operationService = node.nodeEngine.getOperationService();
        Future<Map<String, String>> future =
                operationService.invokeOnTarget(ClientEngineImpl.SERVICE_NAME, operation, node.address);
        Map<String, String> clients = future.get();
        assertEquals(0, clients.size());
    }

    @Test
    public void testGetConnectedClientsOperation_WhenMoreThanZeroClientConnects() throws Exception {
        HecatonInstance instance = factory.newHecatonInstance();

        factory.newHecatonClient();
        factory.newHecatonClient();

        Node node = getNode(instance);
        Operation operation = new GetConnectedClientsOperation();
        OperationService operationService = node.nodeEngine.getOperationService();
        Future<Map<String, String>> future =
                operationService.invokeOnTarget(ClientEngineImpl.SERVICE_NAME, operation, node.address);
        Map<String, String> clients = future.get();
        assertEquals(2, clients.size());
    }
}
