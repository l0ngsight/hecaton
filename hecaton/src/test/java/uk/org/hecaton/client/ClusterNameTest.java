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

import uk.org.hecaton.client.config.ClientConfig;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterNameTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void cleanup() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void test_clusterName_overClientInstance() {
        hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        String name = randomString();
        clientConfig.setInstanceName(name);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        assertEquals(name, client.getName());
    }

    @Test
    public void test_clusterName_overGetConnectedClients() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        String name = randomString();
        clientConfig.setInstanceName(name);
        hecatonFactory.newHecatonClient(clientConfig);

        Collection<Client> connectedClients = instance.getClientService().getConnectedClients();
        Client client = connectedClients.iterator().next();
        assertEquals(name, client.getName());
    }

    @Test
    public void test_clusterName_overClientConnectedEvent() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();
        final CountDownLatch clientConnected = new CountDownLatch(1);

        final AtomicReference<String> clusterName = new AtomicReference<>();
        instance.getClientService().addClientListener(new ClientListener() {
            @Override
            public void clientConnected(Client client) {
                clusterName.set(client.getName());
                clientConnected.countDown();
            }

            @Override
            public void clientDisconnected(Client client) {

            }
        });

        ClientConfig clientConfig = new ClientConfig();
        String name = randomString();
        clientConfig.setInstanceName(name);
        hecatonFactory.newHecatonClient(clientConfig);

        assertOpenEventually(clientConnected);
        assertEquals(name, clusterName.get());
    }

    @Test
    public void test_clusterName_overClientDisconnectedEvent() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();
        final CountDownLatch clientDisconnected = new CountDownLatch(1);

        final AtomicReference<String> clusterName = new AtomicReference<>();
        instance.getClientService().addClientListener(new ClientListener() {
            @Override
            public void clientConnected(Client client) {

            }

            @Override
            public void clientDisconnected(Client client) {
                clusterName.set(client.getName());
                clientDisconnected.countDown();
            }
        });

        ClientConfig clientConfig = new ClientConfig();
        String name = randomString();
        clientConfig.setInstanceName(name);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);
        client.shutdown();

        assertOpenEventually(clientDisconnected);
        assertEquals(name, clusterName.get());
    }
}
