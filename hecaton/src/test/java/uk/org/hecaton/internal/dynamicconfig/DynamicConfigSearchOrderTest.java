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

package uk.org.hecaton.internal.dynamicconfig;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static uk.org.hecaton.spi.properties.ClusterProperty.SEARCH_DYNAMIC_CONFIG_FIRST;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigSearchOrderTest extends HecatonTestSupport {

    private static final String STATIC_WILDCARD_NAME = "my.custom.data.*";
    private static final String DYNAMIC_WILDCARD_NAME = "my.custom.data.cache.*";
    private static final String STATIC_NAME = "my.custom.data.cache.static";
    private static final String DYNAMIC_NAME = "my.custom.data.cache.dynamic";
    private static final String NON_EXISTENT_NAME = "my.custom.data.cache.none";

    private static final int STATIC_MAX_IDLE = 100;
    private static final int DYNAMIC_MAX_IDLE = 200;

    private Config staticHecatonConfig;
    private HecatonInstance hecatonInstance;

    @Before
    public void setUp() {
        staticHecatonConfig = getConfig();
        staticHecatonConfig.setProperty(SEARCH_DYNAMIC_CONFIG_FIRST.getName(), "true");
        String uuid = UUID.randomUUID().toString();
        staticHecatonConfig.setInstanceName(uuid);
        staticHecatonConfig.setClusterName(uuid);
    }

    @Test
    public void testSearchConfigForDynamicWildcardOnlyConfig() {
        hecatonInstance = createHecatonInstance(staticHecatonConfig);
        hecatonInstance.getConfig().addMapConfig(new MapConfig(DYNAMIC_WILDCARD_NAME).setMaxIdleSeconds(DYNAMIC_MAX_IDLE));
        assertEquals("Dynamic wildcard max idle should match", DYNAMIC_MAX_IDLE,
                hecatonInstance.getConfig().getMapConfig(DYNAMIC_NAME).getMaxIdleSeconds());
    }

    @Test
    public void testSearchConfigOrderForDynamicWildcardAndExactConfig() {
        hecatonInstance = createHecatonInstance(staticHecatonConfig);
        hecatonInstance.getConfig().addMapConfig(new MapConfig(DYNAMIC_WILDCARD_NAME).setMaxIdleSeconds(DYNAMIC_MAX_IDLE));
        hecatonInstance.getConfig().addMapConfig(new MapConfig(DYNAMIC_NAME).setMaxIdleSeconds(DYNAMIC_MAX_IDLE));
        assertEquals("Dynamic exact match should prepend wildcard settings", DYNAMIC_MAX_IDLE,
                hecatonInstance.getConfig().getMapConfig(DYNAMIC_NAME).getMaxIdleSeconds());
    }

    @Test
    public void testSearchConfigOrderForDynamicAndStaticConfigs() {
        staticHecatonConfig.addMapConfig(new MapConfig(STATIC_WILDCARD_NAME).setMaxIdleSeconds(STATIC_MAX_IDLE));
        staticHecatonConfig.addMapConfig(new MapConfig(STATIC_NAME).setMaxIdleSeconds(STATIC_MAX_IDLE));
        hecatonInstance = createHecatonInstance(staticHecatonConfig);
        hecatonInstance.getConfig().addMapConfig(new MapConfig(DYNAMIC_WILDCARD_NAME).setMaxIdleSeconds(DYNAMIC_MAX_IDLE));
        hecatonInstance.getConfig().addMapConfig(new MapConfig(DYNAMIC_NAME).setMaxIdleSeconds(DYNAMIC_MAX_IDLE));

        assertEquals("Dynamic exact max idle should match", DYNAMIC_MAX_IDLE,
                hecatonInstance.getConfig().getMapConfig(DYNAMIC_NAME).getMaxIdleSeconds());
        assertEquals("Dynamic wildcard settings should prepend static settings", DYNAMIC_MAX_IDLE,
                hecatonInstance.getConfig().getMapConfig(STATIC_NAME).getMaxIdleSeconds());
        assertEquals("Dynamic wildcard settings should prepend static settings", DYNAMIC_MAX_IDLE,
                hecatonInstance.getConfig().getMapConfig(NON_EXISTENT_NAME).getMaxIdleSeconds());
    }

    @Test
    public void testSearchConfigOrderForStaticConfigs() {
        staticHecatonConfig.addMapConfig(new MapConfig(STATIC_WILDCARD_NAME).setMaxIdleSeconds(STATIC_MAX_IDLE));
        staticHecatonConfig.addMapConfig(new MapConfig(STATIC_NAME).setMaxIdleSeconds(STATIC_MAX_IDLE));
        hecatonInstance = createHecatonInstance(staticHecatonConfig);

        assertEquals("Static wildcard settings should match", STATIC_MAX_IDLE,
                hecatonInstance.getConfig().getMapConfig(NON_EXISTENT_NAME).getMaxIdleSeconds());
        assertEquals("Static exact max idle should match", STATIC_MAX_IDLE,
                hecatonInstance.getConfig().getMapConfig(STATIC_NAME).getMaxIdleSeconds());
    }
}
