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
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.internal.util.FilteringClassLoader;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.config.UserCodeDeploymentConfig.ClassCacheMode.ETERNAL;
import static uk.org.hecaton.config.UserCodeDeploymentConfig.ProviderMode.LOCAL_AND_CACHED_CLASSES;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

// Test resolution of user customization class names in dynamic data structure config via user code deployment
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicConfigAdvancedTest {

    private static final String MAP_NAME = "map-with-maploader";

    private TestHecatonInstanceFactory factory;

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void test_userCustomizations_withUserCodeDeployment() {
        factory = new TestHecatonInstanceFactory();
        // first member is aware of MapLoader class
        HecatonInstance member1 = factory.newHecatonInstance(newConfigWithUserCodeDeployment());
        member1.getConfig().addMapConfig(mapConfigWithMapLoader());

        IMap<Integer, Integer> intMap = member1.getMap(MAP_NAME);
        assertEquals(1, (long) intMap.get(1));

        // start another member which is not aware of map loader
        Config config = newConfigWithUserCodeDeployment();
        FilteringClassLoader cl = new FilteringClassLoader(singletonList("classloading"), null);
        config.setClassLoader(cl);
        factory.newHecatonInstance(config);

        for (int i = 0; i < 1000; i++) {
            assertEquals(i, (long) intMap.get(i));
        }
    }

    private MapConfig mapConfigWithMapLoader() {
        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.getMapStoreConfig()
                .setEnabled(true)
                .setClassName("classloading.domain.IntMapLoader");
        return mapConfig;
    }

    private Config newConfigWithUserCodeDeployment() {
        Config config = new Config();
        config.getUserCodeDeploymentConfig()
                .setEnabled(true)
                .setClassCacheMode(ETERNAL)
                .setProviderMode(LOCAL_AND_CACHED_CLASSES)
                .setWhitelistedPrefixes("classloading");
        return config;
    }

}
