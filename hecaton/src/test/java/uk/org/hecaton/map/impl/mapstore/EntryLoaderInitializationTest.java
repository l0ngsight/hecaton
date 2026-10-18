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
import uk.org.hecaton.map.EntryLoader;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.config.MapStoreConfig.InitialLoadMode.EAGER;
import static uk.org.hecaton.config.MapStoreConfig.InitialLoadMode.LAZY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EntryLoaderInitializationTest extends HecatonTestSupport {

    private static final int ENTRY_COUNT = 10;
    private static final long BIG_TTL = 3600;
    private static final long SMALL_TTL = 5;

    @Test
    public void testEagerEntryLoaderInitializesValues() {
        TestEntryLoader entryLoader = createAndInitializeEntryLoader(ENTRY_COUNT);
        HecatonInstance instance = createHecatonInstance(createConfig(EAGER, entryLoader));
        IMap<String, String> map = instance.getMap(randomMapName());
        assertEquals(1, entryLoader.getLoadAllKeysCallCount());
        assertTrue(0 < entryLoader.getLoadAllCallCount());

        for (int i = 0; i < ENTRY_COUNT; i++) {
            assertEquals("val" + i, map.get("key" + i));
        }
        assertEquals(0, entryLoader.getLoadCallCount());
    }

    @Test
    public void testEagerEntryLoaderInitializesValues_withExpirationTime() {
        TestEntryLoader entryLoader = createAndInitializeEntryLoaderWithExpiry(ENTRY_COUNT);

        HecatonInstance instance = createHecatonInstance(createConfig(EAGER, entryLoader));
        IMap<String, String> map = instance.getMap(randomMapName());
        assertEquals(1, entryLoader.getLoadAllKeysCallCount());
        assertTrue(0 < entryLoader.getLoadAllCallCount());

        for (int i = 0; i < ENTRY_COUNT; i++) {
            assertEquals("val" + i, map.get("key" + i));
        }
        assertEquals(0, entryLoader.getLoadCallCount());
        sleepAtLeastSeconds(SMALL_TTL + 2);
        for (int i = ENTRY_COUNT; i < ENTRY_COUNT * 2; i++) {
            assertNull(map.get("key" + i));
        }

        // entryLoader.loadCallCount() may be greater than entryCount because of retries
        assertGreaterOrEquals("entryLoader.loadCallCount()", entryLoader.getLoadCallCount(), ENTRY_COUNT);
        assertEquals(ENTRY_COUNT, entryLoader.getLoadUniqueKeysCount());
    }

    @Test
    public void testLazyEntryLoaderInitializesAfterAccess() {
        TestEntryLoader entryLoader = createAndInitializeEntryLoader(ENTRY_COUNT);
        HecatonInstance instance = createHecatonInstance(createConfig(LAZY, entryLoader));
        IMap<String, String> map = instance.getMap(randomMapName());
        assertEquals(0, entryLoader.getLoadAllKeysCallCount());
        assertEquals(0, entryLoader.getLoadAllCallCount());

        for (int i = 0; i < ENTRY_COUNT; i++) {
            assertEquals("val" + i, map.get("key" + i));
        }
        assertEquals(0, entryLoader.getLoadCallCount());
        assertTrue(0 < entryLoader.getLoadAllCallCount());
    }

    @Test
    public void testLazyEntryLoaderInitializesAfterAccess_withExpirationTime() {
        TestEntryLoader entryLoader = createAndInitializeEntryLoaderWithExpiry(ENTRY_COUNT);

        HecatonInstance instance = createHecatonInstance(createConfig(LAZY, entryLoader));
        IMap<String, String> map = instance.getMap(randomMapName());
        assertEquals(0, entryLoader.getLoadAllKeysCallCount());
        assertEquals(0, entryLoader.getLoadAllCallCount());

        for (int i = 0; i < ENTRY_COUNT; i++) {
            assertEquals("val" + i, map.get("key" + i));
        }
        assertEquals(0, entryLoader.getLoadCallCount());
        sleepAtLeastSeconds(SMALL_TTL + 2);
        for (int i = ENTRY_COUNT; i < ENTRY_COUNT * 2; i++) {
            assertNull(map.get("key" + i));
        }

        // entryLoader.loadCallCount() may be greater than entryCount because of retries
        assertGreaterOrEquals("entryLoader.loadCallCount()", entryLoader.getLoadCallCount(), ENTRY_COUNT);
        assertEquals(ENTRY_COUNT, entryLoader.getLoadUniqueKeysCount());
    }

    private Config createConfig(MapStoreConfig.InitialLoadMode loadMode, EntryLoader entryLoader) {
        Config config = new Config();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setInitialLoadMode(loadMode);
        mapStoreConfig.setEnabled(true).setImplementation(entryLoader);
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        return config;
    }

    private TestEntryLoader createAndInitializeEntryLoaderWithExpiry(int entryCount) {
        long bigExpiry = System.currentTimeMillis() + BIG_TTL * 1000;
        long smallExpiry = System.currentTimeMillis() + SMALL_TTL * 1000;

        TestEntryLoader entryLoader = new TestEntryLoader();

        for (int i = 0; i < entryCount; i++) {
            entryLoader.putExternally("key" + i, "val" + i, bigExpiry);
        }

        for (int i = entryCount; i < entryCount * 2; i++) {
            entryLoader.putExternally("key" + i, "val" + i, smallExpiry);
        }

        return entryLoader;
    }

    private TestEntryLoader createAndInitializeEntryLoader(int entryCount) {
        TestEntryLoader entryLoader = new TestEntryLoader();
        for (int i = 0; i < entryCount; i++) {
            entryLoader.putExternally("key" + i, "val" + i);
        }
        return entryLoader;
    }
}
