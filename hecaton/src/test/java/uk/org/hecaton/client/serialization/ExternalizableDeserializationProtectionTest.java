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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.IndexType;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JavaSerializationFilterConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.nio.serialization.HecatonSerializationException;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;

import example.serialization.TestExternalizableDeserialized;

/**
 * Tests untrusted deserialization protection for Externalizables.
 *
 * <pre>
 * Given: Hecaton member and clients are started.
 * </pre>
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class ExternalizableDeserializationProtectionTest extends HecatonTestSupport {


    protected static TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @AfterClass
    public static void stopHecatonInstances() {
        hecatonFactory.terminateAll();
    }

    @Before
    public void killAllHecatonInstances() {
        hecatonFactory.terminateAll();
        TestExternalizableDeserialized.isDeserialized = false;
    }

    @Test
    public void testExternalizableProtectedOnMember() {
        JavaSerializationFilterConfig javaSerializationFilterConfig = new JavaSerializationFilterConfig()
                .setDefaultsDisabled(true);
        javaSerializationFilterConfig.getBlacklist().addClasses(TestExternalizableDeserialized.class.getName());

        Config config = smallInstanceConfig();
        config.getSerializationConfig().setJavaSerializationFilterConfig(javaSerializationFilterConfig);
        // the index will force deserialization
        config.getMapConfig("test").addIndexConfig(new IndexConfig(IndexType.HASH, "name"));
        hecatonFactory.newHecatonInstance(config);

        HecatonInstance client = hecatonFactory.newHecatonClient();

        assertThrows(HecatonSerializationException.class,
                () -> client.getMap("test").put("key", new TestExternalizableDeserialized()));
    }

    @Test
    public void testExternalizableProtectedOnClient() {
        JavaSerializationFilterConfig javaSerializationFilterConfig = new JavaSerializationFilterConfig()
                .setDefaultsDisabled(true);
        javaSerializationFilterConfig.getBlacklist().addClasses(TestExternalizableDeserialized.class.getName());

        Config config = smallInstanceConfig();
        hecatonFactory.newHecatonInstance(config);

        ClientConfig clientConfig1 = new ClientConfig();
        HecatonInstance client1 = hecatonFactory.newHecatonClient(clientConfig1);
        client1.getMap("test").put("key", new TestExternalizableDeserialized());
        // we don't have an index on map, so the value should not be deserialized
        assertFalse(TestExternalizableDeserialized.isDeserialized);
        // deserialized on client
        client1.getMap("test").get("key");
        assertTrue(TestExternalizableDeserialized.isDeserialized);
        TestExternalizableDeserialized.isDeserialized = false;

        ClientConfig clientConfig2 = new ClientConfig();
        clientConfig2.getSerializationConfig().setJavaSerializationFilterConfig(javaSerializationFilterConfig);
        HecatonInstance client2 = hecatonFactory.newHecatonClient(clientConfig2);

        assertThrows(HecatonSerializationException.class, () -> client2.getMap("test").get("key"));
    }

    @Test
    public void testExternalizableUnprotected() {
        Config config = smallInstanceConfig();
        config.getMapConfig("test").addIndexConfig(new IndexConfig(IndexType.HASH, "name"));
        hecatonFactory.newHecatonInstance(config);

        HecatonInstance client = hecatonFactory.newHecatonClient();

        client.getMap("test").put("key", new TestExternalizableDeserialized());
        assertTrue(TestExternalizableDeserialized.isDeserialized);
        TestExternalizableDeserialized.isDeserialized = false;
        client.getMap("test").get("key");
        assertTrue(TestExternalizableDeserialized.isDeserialized);
    }
}
