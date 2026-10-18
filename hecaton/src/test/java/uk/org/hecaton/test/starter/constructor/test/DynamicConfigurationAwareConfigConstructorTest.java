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

package uk.org.hecaton.test.starter.constructor.test;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ListConfig;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.test.starter.constructor.DynamicConfigurationAwareConfigConstructor;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.HecatonTestSupport.assertPropertiesEquals;
import static uk.org.hecaton.test.starter.constructor.test.ConfigConstructorTest.buildPropertiesWithDefaults;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigurationAwareConfigConstructorTest {

    @Test
    public void testConstructor() {
        Config config = new Config()
                .setInstanceName("myInstanceName")
                .addMapConfig(new MapConfig("myMap"))
                .addListConfig(new ListConfig("myList"))
                .addListenerConfig(new ListenerConfig("uk.org.hecaton.test.MyListenerConfig"))
                .setProperties(buildPropertiesWithDefaults());

        HecatonProperties properties = new HecatonProperties(config);

        DynamicConfigurationAwareConfig dynamicConfig = new DynamicConfigurationAwareConfig(config, properties);

        DynamicConfigurationAwareConfigConstructor constructor
                = new DynamicConfigurationAwareConfigConstructor(DynamicConfigurationAwareConfig.class);
        DynamicConfigurationAwareConfig clonedDynamicConfig
                = (DynamicConfigurationAwareConfig) constructor.createNew(dynamicConfig);

        assertEquals(dynamicConfig.getInstanceName(), clonedDynamicConfig.getInstanceName());
        assertEquals(dynamicConfig.getMapConfigs().size(), clonedDynamicConfig.getMapConfigs().size());
        assertEquals(dynamicConfig.getListConfigs().size(), clonedDynamicConfig.getListConfigs().size());
        assertEquals(dynamicConfig.getListenerConfigs().size(), clonedDynamicConfig.getListenerConfigs().size());
        assertPropertiesEquals(dynamicConfig.getProperties(), clonedDynamicConfig.getProperties());
    }
}
