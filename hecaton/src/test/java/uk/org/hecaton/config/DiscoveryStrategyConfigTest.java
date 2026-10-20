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

package uk.org.hecaton.config;

import uk.org.hecaton.config.properties.PropertyDefinition;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.discovery.DiscoveryNode;
import uk.org.hecaton.spi.discovery.DiscoveryStrategy;
import uk.org.hecaton.spi.discovery.DiscoveryStrategyFactory;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DiscoveryStrategyConfigTest {

    @Test
    public void test_DiscoveryStrategyFactory_constructor_classname() {
        String discoveryStrategyFactoryClass = TestDiscoveryStrategyFactory.class.getName();
        DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig(discoveryStrategyFactoryClass);
        assertEquals(discoveryStrategyFactoryClass, discoveryStrategyConfig.getClassName());
        assertNull(discoveryStrategyConfig.getDiscoveryStrategyFactory());
        assertEquals(0, discoveryStrategyConfig.getProperties().size());
    }

    @Test
    public void test_DiscoveryStrategyFactory_constructor_classname_properties() {
        String discoveryStrategyFactoryClass = TestDiscoveryStrategyFactory.class.getName();
        Map<String, Comparable> properties = new HashMap<>();
        properties.put("test", "value");

        DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig(discoveryStrategyFactoryClass, properties);
        assertEquals(discoveryStrategyFactoryClass, discoveryStrategyConfig.getClassName());
        assertNull(discoveryStrategyConfig.getDiscoveryStrategyFactory());
        assertEquals(1, discoveryStrategyConfig.getProperties().size());
        assertEquals(properties, discoveryStrategyConfig.getProperties());
    }

    @Test
    public void test_DiscoveryStrategyFactory_constructor_factory() {
        DiscoveryStrategyFactory discoveryStrategyFactory = new TestDiscoveryStrategyFactory();
        DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig(discoveryStrategyFactory);
        assertSame(discoveryStrategyFactory, discoveryStrategyConfig.getDiscoveryStrategyFactory());
        assertNull(discoveryStrategyConfig.getClassName());
        assertEquals(0, discoveryStrategyConfig.getProperties().size());
    }

    @Test
    public void test_DiscoveryStrategyFactory_constructor_factory_properties() {
        DiscoveryStrategyFactory discoveryStrategyFactory = new TestDiscoveryStrategyFactory();
        Map<String, Comparable> properties = new HashMap<>();
        properties.put("test", "value");

        DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig(discoveryStrategyFactory, properties);
        assertSame(discoveryStrategyFactory, discoveryStrategyConfig.getDiscoveryStrategyFactory());
        assertNull(discoveryStrategyConfig.getClassName());
        assertEquals(1, discoveryStrategyConfig.getProperties().size());
        assertEquals(properties, discoveryStrategyConfig.getProperties());
    }

    @Test
    public void test_DiscoveryStrategyFactory_properties_add_remove() {
        DiscoveryStrategyFactory discoveryStrategyFactory = new TestDiscoveryStrategyFactory();
        DiscoveryStrategyConfig discoveryStrategyConfig = new DiscoveryStrategyConfig(discoveryStrategyFactory);
        assertEquals(0, discoveryStrategyConfig.getProperties().size());

        discoveryStrategyConfig.addProperty("test", "value");
        assertEquals(1, discoveryStrategyConfig.getProperties().size());
        assertEquals("value", discoveryStrategyConfig.getProperties().get("test"));

        discoveryStrategyConfig.removeProperty("test");
        assertEquals(0, discoveryStrategyConfig.getProperties().size());
    }

    private static class TestDiscoveryStrategyFactory implements DiscoveryStrategyFactory {

        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return null;
        }

        @Override
        public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger,
                                                      Map<String, Comparable> properties) {
            return null;
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return null;
        }
    }
}
