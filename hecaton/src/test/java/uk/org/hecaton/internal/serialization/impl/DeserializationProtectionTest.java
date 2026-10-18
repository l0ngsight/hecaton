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

package uk.org.hecaton.internal.serialization.impl;

import uk.org.hecaton.config.ClassFilter;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JavaSerializationFilterConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.nio.serialization.HecatonSerializationException;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.QuickTest;
import example.serialization.TestDeserialized;
import org.junit.After;
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
 * Given: 2 node cluster is started.
 * </pre>
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class DeserializationProtectionTest extends HecatonTestSupport {

    @Before
    @After
    public void killAllHecatonInstances() {
        TestDeserialized.isDeserialized = false;
    }

    /**
     * Test default filter configuration.
     *
     * <pre>
     * When: An untrusted serialized object is stored on target Hecaton instance and default Whitelist is used.
     * Then: Deserialization fails.
     * </pre>
     */
    @Test
    public void testDefaultDeserializationFilter_keyNotOwnedByTarget() {
        assertDeserializationFails(new JavaSerializationFilterConfig(), true);
    }

    /**
     * <pre>
     * When: An untrusted serialized object is stored on source Hecaton instance and default Whitelist is used.
     * Then: Deserialization fails.
     * </pre>
     */
    @Test
    public void testDefaultDeserializationFilter_keyOwnedBySource() {
        assertDeserializationFails(new JavaSerializationFilterConfig(), false);
    }

    /**
     * <pre>
     * When: Default Whitelist is disabled and classname of the test serialized object is blacklisted.
     * Then: Deserialization fails.
     * </pre>
     */
    @Test
    public void testClassBlacklisted() {
        ClassFilter blacklist = new ClassFilter().addClasses(TestDeserialized.class.getName());
        JavaSerializationFilterConfig filterConfig = new JavaSerializationFilterConfig().setDefaultsDisabled(true)
                .setBlacklist(blacklist);
        assertDeserializationFails(filterConfig, false);
    }

    /**
     * <pre>
     * When: Deserialization filtering is not explicitly enabled.
     * Then: Untrusted deserialization is possible.
     * </pre>
     */
    @Test
    public void testNoDeserializationFilter() {
        assertDeserializationPass(null);
    }

    /**
     * <pre>
     * When: Deserialization filtering is enabled and classname of test object is whitelisted.
     * Then: The deserialization is possible.
     * </pre>
     */
    @Test
    public void testClassWhitelisted() {
        JavaSerializationFilterConfig filterConfig = new JavaSerializationFilterConfig();
        filterConfig.getWhitelist().addClasses(TestDeserialized.class.getName());
        assertDeserializationPass(filterConfig);
    }

    private void assertDeserializationFails(JavaSerializationFilterConfig javaSerializationFilterConfig,
                                            boolean keyOwnedByTarget) {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        Config config = new Config();
        config.getSerializationConfig().setJavaSerializationFilterConfig(javaSerializationFilterConfig);
        HecatonInstance[] instances = factory.newInstances(config);
        String key = generateKeyOwnedBy(instances[keyOwnedByTarget ? 1 : 0]);
        instances[0].getMap("test").put(key, new TestDeserialized());
        try {
            instances[1].getMap("test").get(key);
            fail("Deserialization should have failed");
        } catch (HecatonSerializationException e) {
            assertFalse(TestDeserialized.isDeserialized);
        }
    }

    private void assertDeserializationPass(JavaSerializationFilterConfig filterConfig) {
        Config config = new Config();
        config.getSerializationConfig().setJavaSerializationFilterConfig(filterConfig);
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        HecatonInstance[] instances = factory.newInstances(config);
        instances[0].getMap("test").put("a", new TestDeserialized());
        assertNotNull(instances[1].getMap("test").get("a"));
        assertTrue(TestDeserialized.isDeserialized);
    }

}
