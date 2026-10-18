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

package uk.org.hecaton.map.impl.mapstore;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.MapLoader;
import uk.org.hecaton.map.MapLoaderLifecycleSupport;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapLoaderLifecycleTest extends HecatonTestSupport {

    private final MapLoaderLifecycleSupport loader = mockMapLoaderWithLifecycle();
    private final Config config = new Config();

    @Before
    public void configure() {
        config.getMapConfig("map").setMapStoreConfig(new MapStoreConfig().setImplementation(loader));
    }

    @Test
    public void testInitCalled_whenMapCreated() {

        HecatonInstance hz = createHecatonInstance(config);

        IMap<String, String> map = hz.getMap("map");
        // MapStore creation is deferred, so trigger map store creation by putting some data in the map
        map.put("a", "b");

        verify(loader).init(eq(hz), eq(new Properties()), eq("map"));
    }

    @Test
    public void testDestroyCalled_whenNodeShutdown() {

        HecatonInstance hz = createHecatonInstance(config);

        IMap<String, String> map = hz.getMap("map");
        // MapStore creation is deferred, so trigger map store creation by putting some data in the map
        map.put("a", "b");

        hz.shutdown();

        verify(loader).destroy();
    }

    private static MapLoaderLifecycleSupport mockMapLoaderWithLifecycle() {
        return mock(MapLoaderLifecycleSupport.class, withSettings().extraInterfaces(MapLoader.class));
    }
}
