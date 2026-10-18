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
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.test.starter.constructor.ConfigConstructor;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static uk.org.hecaton.test.HecatonTestSupport.assertPropertiesEquals;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConfigConstructorTest {

    @Test
    public void testConstructor() {
        Config config = new Config()
                .setInstanceName("myInstanceName")
                .addMapConfig(new MapConfig("myMap"))
                .addListConfig(new ListConfig("myList"))
                .addListenerConfig(new ListenerConfig("uk.org.hecaton.test.MyListenerConfig"))
                .setProperties(buildPropertiesWithDefaults());

        ConfigConstructor constructor = new ConfigConstructor(Config.class);
        Config clonedConfig = (Config) constructor.createNew(config);

        assertEquals(config.getInstanceName(), clonedConfig.getInstanceName());
        assertEquals(config.getMapConfigs().size(), clonedConfig.getMapConfigs().size());
        assertEquals(config.getListConfigs().size(), clonedConfig.getListConfigs().size());
        assertEquals(config.getListenerConfigs().size(), clonedConfig.getListenerConfigs().size());
        assertPropertiesEquals(config.getProperties(), clonedConfig.getProperties());
    }

    static Properties buildPropertiesWithDefaults() {
        Properties defaults = new Properties();
        defaults.setProperty("key1", "value1");

        Properties configProperties = new Properties(defaults);
        configProperties.setProperty("key2", "value2");

        return configProperties;
    }
}
