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
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class HecatonClientTest extends HecatonTestSupport {

    private static final String CLIENT_CONFIG_PROP_NAME = "hecaton.client.config";

    @Before
    public void setUp() {
        cleanup();
        Hecaton.newHecatonInstance(smallInstanceConfig());
    }

    @After
    public void tearDown() {
        cleanup();
    }

    private void cleanup() {
        HecatonClient.shutdownAll();
        assertEquals(0, HecatonClient.getAllHecatonClients().size());
        Hecaton.shutdownAll();
        assertEquals(0, Hecaton.getAllHecatonInstances().size());
    }

    // null instance name
    @Test(expected = IllegalArgumentException.class)
    public void testGetOrCreateHecatonClient_withNullConfig() {
        HecatonClient.getOrCreateHecatonClient(null);
    }

    @Test
    public void testGetOrCreateHecatonClient_returnsSame_withNoConfig() {
        String hzConfigProperty = System.getProperty(CLIENT_CONFIG_PROP_NAME);
        try {
            System.setProperty(CLIENT_CONFIG_PROP_NAME, "classpath:hecaton-client-test.xml");
            HecatonInstance client1 = HecatonClient.getOrCreateHecatonClient();
            HecatonInstance client2 = HecatonClient.getOrCreateHecatonClient();
            assertEquals("Calling two times getOrCreateHecatonClient should return same client", client1, client2);
        } finally {
            if (hzConfigProperty == null) {
                System.clearProperty(CLIENT_CONFIG_PROP_NAME);
            } else {
                System.setProperty(CLIENT_CONFIG_PROP_NAME, hzConfigProperty);
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetOrCreateHecatonClient_withNullName() {
        ClientConfig config = new ClientConfig();
        HecatonClient.getOrCreateHecatonClient(config);
    }

    @Test
    public void testGetOrCreateHecatonClient_whenClientDoesNotExist() {
        String instanceName = randomString();
        ClientConfig config = new ClientConfig();
        config.setInstanceName(instanceName);

        HecatonInstance client = HecatonClient.getOrCreateHecatonClient(config);
        assertEquals(client, HecatonClient.getHecatonClientByName(instanceName));
    }

    @Test
    public void testGetOrCreateHecatonClient_whenClientExists() {
        String instanceName = randomString();
        ClientConfig config = new ClientConfig();
        config.setInstanceName(instanceName);

        HecatonInstance client = HecatonClient.newHecatonClient(config);
        assertEquals(client, HecatonClient.getOrCreateHecatonClient(config));
    }

    @Test
    public void testGetOrCreateHecatonClientConcurrently() throws ExecutionException, InterruptedException {
        String instanceName = randomString();
        ClientConfig config = new ClientConfig();
        config.setInstanceName(instanceName);

        int clientCount = 10;
        List<HecatonInstance> clients = Collections.synchronizedList(new ArrayList<>(clientCount));
        List<Future<?>> futures = new ArrayList<>(clientCount);
        for (int i = 0; i < clientCount; i++) {
            futures.add(spawn(() -> {
                clients.add(HecatonClient.getOrCreateHecatonClient(config));
            }));
        }
        for (int i = 0; i < clientCount; i++) {
            futures.get(i).get();
        }
        assertEquals(clientCount, clients.size());
        for (int i = 1; i < clientCount; i++) {
            assertEquals(clients.get(0), clients.get(i));
        }
    }

    @Test
    public void testGetHecatonClientByName() {
        String instanceName = randomString();
        ClientConfig config = new ClientConfig();
        config.setInstanceName(instanceName);

        HecatonInstance client1 = HecatonClient.newHecatonClient(config);
        assertEquals(client1, HecatonClient.getHecatonClientByName(instanceName));
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testNewHecatonClient_withSameConfig() {
        String instanceName = randomString();
        ClientConfig config = new ClientConfig();
        config.setInstanceName(instanceName);

        HecatonClient.newHecatonClient(config);
        HecatonClient.newHecatonClient(config);
    }

    @Test
    public void testGetAllHecatonClients() {
        HecatonInstance client1 = HecatonClient.newHecatonClient(new ClientConfig()
                .setInstanceName(randomString()));
        HecatonInstance client2 = HecatonClient.getOrCreateHecatonClient(new ClientConfig()
                .setInstanceName(randomString()));

        Collection<HecatonInstance> clients = HecatonClient.getAllHecatonClients();
        assertEquals(2, clients.size());
        assertContains(clients, client1);
        assertContains(clients, client2);
    }

    @Test
    public void testShutdownAll() {
        HecatonClient.newHecatonClient(new ClientConfig()
                .setInstanceName(randomString()));
        HecatonClient.getOrCreateHecatonClient(new ClientConfig()
                .setInstanceName(randomString()));

        HecatonClient.shutdownAll();
        assertEquals(0, HecatonClient.getAllHecatonClients().size());
    }

    @Test
    public void testShutdown_withInstance() {
        HecatonInstance client = HecatonClient.newHecatonClient(new ClientConfig()
                .setInstanceName(randomString()));

        HecatonClient.shutdown(client);

        assertEquals(0, HecatonClient.getAllHecatonClients().size());
        assertFalse(client.getLifecycleService().isRunning());
    }

    public void testShutdown_withName() {
        ClientConfig config = new ClientConfig();
        String instanceName = randomString();
        config.setInstanceName(instanceName);

        HecatonInstance client = HecatonClient.newHecatonClient(config);
        HecatonClient.shutdown(instanceName);

        assertEquals(0, HecatonClient.getAllHecatonClients().size());
        assertFalse(client.getLifecycleService().isRunning());
    }
}
