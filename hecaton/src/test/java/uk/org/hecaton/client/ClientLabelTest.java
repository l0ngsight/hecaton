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
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static uk.org.hecaton.test.HecatonTestSupport.assertContains;
import static uk.org.hecaton.test.HecatonTestSupport.assertNotContains;
import static uk.org.hecaton.test.HecatonTestSupport.assertOpenEventually;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientLabelTest {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();
    private final String label = "attributeValue";
    private final String nonExistingLabel = "nonExistingLabel";

    @After
    public void cleanup() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void test_clientLabel_overClientConfig() {
        ClientConfig clientConfig = new ClientConfig();

        clientConfig.addLabel(label);
        assertTrue(clientConfig.getLabels().contains(label));
        assertEquals(1, clientConfig.getLabels().size());
        assertFalse(clientConfig.getLabels().contains(nonExistingLabel));
    }

    @Test
    public void test_clientLabel_overGetConnectedClients() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addLabel(label);
        hecatonFactory.newHecatonClient(clientConfig);

        Collection<Client> connectedClients = instance.getClientService().getConnectedClients();
        Client client = connectedClients.iterator().next();

        assertEquals(1, client.getLabels().size());
        assertContains(client.getLabels(), label);
        assertNotContains(client.getLabels(), nonExistingLabel);
    }

    @Test
    public void test_clientAttribute_overClientConnectedEvent() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();
        final CountDownLatch clientConnected = new CountDownLatch(1);

        final AtomicReference<Client> clientRef = new AtomicReference<>();
        instance.getClientService().addClientListener(new ClientListener() {
            @Override
            public void clientConnected(Client client) {
                clientRef.set(client);
                clientConnected.countDown();
            }

            @Override
            public void clientDisconnected(Client client) {

            }
        });

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addLabel(label);
        hecatonFactory.newHecatonClient(clientConfig);

        assertOpenEventually(clientConnected);
        Client client = clientRef.get();

        assertEquals(1, client.getLabels().size());
        assertContains(client.getLabels(), label);
        assertNotContains(client.getLabels(), nonExistingLabel);
    }

    @Test
    public void test_clientAttribute_overClientDisconnectedEvent() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();
        final CountDownLatch clientDisconnected = new CountDownLatch(1);

        final AtomicReference<Client> clientRef = new AtomicReference<>();
        instance.getClientService().addClientListener(new ClientListener() {
            @Override
            public void clientConnected(Client client) {

            }

            @Override
            public void clientDisconnected(Client client) {
                clientRef.set(client);
                clientDisconnected.countDown();
            }
        });

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addLabel(label);
        HecatonInstance clientInstance = hecatonFactory.newHecatonClient(clientConfig);
        clientInstance.shutdown();

        assertOpenEventually(clientDisconnected);
        Client client = clientRef.get();

        assertEquals(1, client.getLabels().size());
        assertContains(client.getLabels(), label);
        assertNotContains(client.getLabels(), nonExistingLabel);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void test_modifyClientLabels_overGetConnectedClients() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addLabel(label);
        hecatonFactory.newHecatonClient(clientConfig);

        Collection<Client> connectedClients = instance.getClientService().getConnectedClients();
        Client client = connectedClients.iterator().next();
        client.getLabels().add(label);
    }
}
