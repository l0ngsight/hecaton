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

package uk.org.hecaton.client.config;

import uk.org.hecaton.client.LoadBalancer;
import uk.org.hecaton.client.test.Employee;
import uk.org.hecaton.client.test.PortableFactory;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.client.util.RandomLB;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.SerializationConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientConfigTest {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testCopyConstructor_withFullyConfiguredClientConfig() throws IOException {
        URL schemaResource = ClientConfigTest.class.getClassLoader().getResource("hecaton-client-full.xml");
        ClientConfig expected = new XmlClientConfigBuilder(schemaResource).build();
        ClientConfig actual = new ClientConfig(expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testCopyConstructor_withDefaultClientConfig() {
        ClientConfig expected = new ClientConfig();
        ClientConfig actual = new ClientConfig(expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testAccessDefaultCluster() {
        hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient();

        assertNotNull(client.getConfig());
    }

    @Test
    public void testAccessClusterNameOverClientInstance() {
        Config config = new Config();
        String clusterName = "aClusterName";
        config.setClusterName(clusterName);
        hecatonFactory.newHecatonInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(clusterName);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        assertEquals(clusterName, client.getConfig().getClusterName());
    }

    @Test
    public void testAccessSerializationConfigOverClientInstance() {
        hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().addPortableFactory(PortableFactory.FACTORY_ID, new PortableFactory());
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        SerializationConfig serializationConfig = client.getConfig().getSerializationConfig();
        Map<Integer, uk.org.hecaton.nio.serialization.PortableFactory> factories = serializationConfig.getPortableFactories();
        assertEquals(1, factories.size());
        assertEquals(Employee.CLASS_ID, factories.get(PortableFactory.FACTORY_ID).create(Employee.CLASS_ID).getClassId());
    }

    @Test
    public void testUserContext_passContext() {
        hecatonFactory.newHecatonInstance();

        ClientConfig clientConfig = new ClientConfig();
        ConcurrentMap<String, Object> context = new ConcurrentHashMap<>();
        context.put("key1", "value1");
        Object value2 = new Object();
        context.put("key2", value2);

        // check set setter returns ClientConfig instance
        clientConfig = clientConfig.setUserContext(context);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        ConcurrentMap<String, Object> returnedContext = client.getUserContext();
        assertEquals(context, returnedContext);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUserContext_throwExceptionWhenContextNull() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setUserContext(null);
    }

    @Test
    public void testReliableTopic() {
        ClientConfig clientConfig = new ClientConfig();
        ClientReliableTopicConfig defaultReliableTopicConfig = new ClientReliableTopicConfig("default");
        defaultReliableTopicConfig.setReadBatchSize(100);
        clientConfig.addReliableTopicConfig(defaultReliableTopicConfig);
        ClientReliableTopicConfig newConfig = clientConfig.getReliableTopicConfig("newConfig");

        assertEquals(100, newConfig.getReadBatchSize());
    }

    @Test
    public void testSettingLoaderBalancerShouldClearLoadBalancerClassName() {
        LoadBalancer loadBalancer = new RandomLB();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLoadBalancerClassName("uk.org.hecaton.client.test.CustomLoadBalancer");
        clientConfig.setLoadBalancer(loadBalancer);

        assertNull(clientConfig.getLoadBalancerClassName());
        assertSame(loadBalancer, clientConfig.getLoadBalancer());
    }

    @Test
    public void testSettingLoadBalancerClassNameShouldClearLoadBalancer() {
        LoadBalancer loadBalancer = new RandomLB();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLoadBalancer(loadBalancer);
        clientConfig.setLoadBalancerClassName("uk.org.hecaton.client.test.CustomLoadBalancer");

        assertEquals("uk.org.hecaton.client.test.CustomLoadBalancer", clientConfig.getLoadBalancerClassName());
        assertNull(clientConfig.getLoadBalancer());
    }

    @Test
    public void testTpcConfig() {
        ClientConfig config = new ClientConfig();
        ClientTpcConfig tpcConfig = new ClientTpcConfig();

        assertFalse(tpcConfig.isEnabled());

        tpcConfig.setEnabled(true);
        tpcConfig.setConnectionCount(10);
        config.setTpcConfig(tpcConfig);

        assertTrue(tpcConfig.isEnabled());
        assertEquals(10, tpcConfig.getConnectionCount());
        assertThrows(IllegalArgumentException.class, () -> config.setTpcConfig(null));
    }

    @Test
    public void testClusterName_throwExceptionWhenNull() {
        ClientConfig clientConfig = new ClientConfig();
        assertThrows(IllegalArgumentException.class, () -> clientConfig.setClusterName(null));
    }
}
