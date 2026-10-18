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

package uk.org.hecaton.client.serialization;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.ClassFilter;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JavaSerializationFilterConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.nio.serialization.HecatonSerializationException;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import example.serialization.TestDeserialized;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests untrusted deserialization protection.
 *
 * <pre>
 * Given: Hecaton member and clients are started.
 * </pre>
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientDeserializationProtectionTest extends HecatonTestSupport {

    protected static TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @AfterClass
    public static final void stopHecatonInstances() {
        hecatonFactory.terminateAll();
    }

    @Before
    public void killAllHecatonInstances() {
        hecatonFactory.terminateAll();
        TestDeserialized.isDeserialized = false;
    }

    /**
     * <pre>
     * When: An untrusted serialized object is stored from client and read from member, the default Whitelist is used.
     * Then: Deserialization fails.
     * </pre>
     */
    @Test
    public void testDefaultDeserializationFilter_readOnMember() {
        JavaSerializationFilterConfig filterConfig = new JavaSerializationFilterConfig();
        Config config = new Config();
        config.getSerializationConfig().setJavaSerializationFilterConfig(filterConfig);
        HecatonInstance member = hecatonFactory.newInstances(config, 1)[0];
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().setJavaSerializationFilterConfig(filterConfig);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        client.getMap("test").put("key", new TestDeserialized());
        try {
            member.getMap("test").get("key");
            fail("Deserialization should have failed");
        } catch (HecatonSerializationException e) {
            assertFalse(TestDeserialized.isDeserialized);
        }
    }

    /**
     * <pre>
     * When: An untrusted serialized object is stored by member and read from client, the default Whitelist is used.
     * Then: Deserialization fails.
     * </pre>
     */
    @Test
    public void testDefaultDeserializationFilter_readOnClient() {
        JavaSerializationFilterConfig filterConfig = new JavaSerializationFilterConfig();
        Config config = new Config();
        config.getSerializationConfig().setJavaSerializationFilterConfig(filterConfig);
        HecatonInstance member = hecatonFactory.newInstances(config, 1)[0];
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().setJavaSerializationFilterConfig(filterConfig);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        member.getMap("test").put("key", new TestDeserialized());
        try {
            client.getMap("test").get("key");
            fail("Deserialization should have failed");
        } catch (HecatonSerializationException e) {
            assertFalse(TestDeserialized.isDeserialized);
        }
    }

    /**
     * <pre>
     * When: Default Whitelist is disabled and class name of the test serialized object is blacklisted. The object is read from client.
     * Then: Deserialization fails.
     * </pre>
     */
    @Test
    public void testClassBlacklisted() {
        ClassFilter blacklist = new ClassFilter().addClasses(TestDeserialized.class.getName());
        JavaSerializationFilterConfig filterConfig = new JavaSerializationFilterConfig().setDefaultsDisabled(true)
                .setBlacklist(blacklist);

        Config config = new Config();
        config.getSerializationConfig().setJavaSerializationFilterConfig(filterConfig);
        HecatonInstance member = hecatonFactory.newInstances(config, 1)[0];
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().setJavaSerializationFilterConfig(filterConfig);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        member.getMap("test").put("key", new TestDeserialized());
        try {
            client.getMap("test").get("key");
            fail("Deserialization should have failed");
        } catch (HecatonSerializationException e) {
            assertFalse(TestDeserialized.isDeserialized);
        }
    }

    /**
     * <pre>
     * When: Deserialization filtering is not explicitly enabled and object is read from client.
     * Then: Untrusted deserialization is possible.
     * </pre>
     */
    @Test
    public void testNoDeserializationFilter() {
        Config config = new Config();
        HecatonInstance member = hecatonFactory.newInstances(config, 1)[0];
        ClientConfig clientConfig = new ClientConfig();
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        member.getMap("test").put("key", new TestDeserialized());
        assertNotNull(client.getMap("test").get("key"));
        assertTrue(TestDeserialized.isDeserialized);
    }

    /**
     * <pre>
     * When: Deserialization filtering is enabled and class name of test object is whitelisted.
     * Then: The deserialization is possible.
     * </pre>
     */
    @Test
    public void testClassWhitelisted() {
        JavaSerializationFilterConfig filterConfig = new JavaSerializationFilterConfig();
        filterConfig.getWhitelist().addClasses(TestDeserialized.class.getName());
        Config config = new Config();
        config.getSerializationConfig().setJavaSerializationFilterConfig(filterConfig);
        HecatonInstance member = hecatonFactory.newInstances(config, 1)[0];
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().setJavaSerializationFilterConfig(filterConfig);
        HecatonInstance client = hecatonFactory.newHecatonClient(clientConfig);

        member.getMap("test").put("key", new TestDeserialized());
        assertNotNull(client.getMap("test").get("key"));
        assertTrue(TestDeserialized.isDeserialized);
    }
}
