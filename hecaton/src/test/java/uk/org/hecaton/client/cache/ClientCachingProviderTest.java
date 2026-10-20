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

package uk.org.hecaton.client.cache;

import uk.org.hecaton.cache.CachingProviderTest;
import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.XmlClientConfigBuilder;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import org.junit.After;
import org.junit.Before;

import javax.cache.spi.CachingProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static uk.org.hecaton.cache.CacheTestSupport.createClientCachingProvider;
import static uk.org.hecaton.cache.jsr.JsrTestUtil.clearCachingProviderRegistry;
import static uk.org.hecaton.cache.jsr.JsrTestUtil.clearSystemProperties;
import static org.junit.Assert.assertNotNull;

public class ClientCachingProviderTest extends CachingProviderTest {

    private static final String CONFIG_CLASSPATH_LOCATION = "test-hecaton-client-jcache.xml";
    private final List<HecatonInstance> instances = new ArrayList<>();

    @Override
    protected String getConfigClasspathLocation() {
        return CONFIG_CLASSPATH_LOCATION;
    }

    @Override
    protected String getProviderType() {
        return "client";
    }

    @Before
    @Override
    public void setup() {
        // start a member
        Config config = new Config();
        HecatonInstance instance = Hecaton.newHecatonInstance(config);
        instances.add(instance);
        // start two client instances
        instance1 = createHecatonInstance(INSTANCE_1_NAME);
        instance2 = createHecatonInstance(INSTANCE_2_NAME);
        try {
            instance3 = HecatonClient.newHecatonClient(new XmlClientConfigBuilder(CONFIG_CLASSPATH_LOCATION).build());
        } catch (IOException e) {
            throw new AssertionError("Could not construct named hecaton client instance: " + e.getMessage());
        }
        instances.add(instance3);
        cachingProvider = createCachingProvider(instance1);
    }

    @Override
    protected HecatonInstance createHecatonInstance(String instanceName) {
        // Since `HecatonClient.getHecatonClientByName(instanceName);` doesn't work on mock client,
        // we are using real instances.
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setInstanceName(instanceName);
        HecatonInstance instance = HecatonClient.newHecatonClient(clientConfig);
        instances.add(instance);
        return instance;
    }

    @Override
    protected CachingProvider createCachingProvider(HecatonInstance defaultInstance) {
        return createClientCachingProvider(defaultInstance);
    }

    @Override
    protected void assertInstanceStarted(String instanceName) {
        HecatonInstance otherInstance = HecatonClient.getHecatonClientByName(instanceName);
        assertNotNull(otherInstance);
        otherInstance.getLifecycleService().terminate();
    }

    @Override
    protected Collection<HecatonInstance> getStartedInstances() {
        return HecatonClient.getAllHecatonClients();
    }

    @Override
    protected void cleanupForDefaultCacheManagerTest() {
        clearSystemProperties();
        clearCachingProviderRegistry();
    }

    @After
    public void tearDown() {
        Iterator<HecatonInstance> iter = instances.iterator();
        while (iter.hasNext()) {
            HecatonInstance instance = iter.next();
            try {
                instance.shutdown();
            } catch (Throwable t) {
                t.printStackTrace();
            } finally {
                iter.remove();
            }
        }
        HecatonClient.shutdownAll();
    }

}
