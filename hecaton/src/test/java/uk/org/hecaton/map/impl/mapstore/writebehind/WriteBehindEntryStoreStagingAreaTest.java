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

package uk.org.hecaton.map.impl.mapstore.writebehind;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.EntryStore;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteBehindEntryStoreStagingAreaTest extends HecatonTestSupport {

    @Test
    public void testEntryLoadedFromStagingAreaEnforcesExpirationTime() {
        final String mapName = randomMapName();
        TemporaryBlockerEntryStore entryStore = new TemporaryBlockerEntryStore();
        HecatonInstance instance = createHecatonInstance(getConfig(mapName, entryStore, 1));
        IMap<String, String> map = instance.getMap(mapName);
        map.put("key", "value");
        // "value" is never stored in EntryStore because we do not grant a store permit
        map.evict("key");
        sleepAtLeastSeconds(2);
        // this value is retrieved from staging area and should be expired
        assertNull(map.get("key"));
    }

    @Test
    public void testEntryLoadedFromStagingAreaEnforcesExpirationTime_loadAll() {
        final String mapName = randomMapName();
        final int entryCount = 100;
        Map<String, String> localMap = new HashMap<>();
        for (int i = 0; i < entryCount; i++) {
            localMap.put("key" + i, "value" + i);
        }
        TemporaryBlockerEntryStore entryStore = new TemporaryBlockerEntryStore();
        HecatonInstance instance = createHecatonInstance(getConfig(mapName, entryStore, 1));
        IMap<String, String> map = instance.getMap(mapName);
        map.putAll(localMap);
        map.evictAll();
        sleepAtLeastSeconds(2);
        Map<String, String> stagedEntries = map.getAll(localMap.keySet());
        for (int i = 0; i < entryCount; i++) {
            assertNull(stagedEntries.get("key" + i));
        }
    }

    @Test
    public void testEntryLoadedFromStagingAreaEnforcesNoExpirationTime() {
        final String mapName = randomMapName();
        TemporaryBlockerEntryStore entryStore = new TemporaryBlockerEntryStore();
        HecatonInstance instance = createHecatonInstance(getConfig(mapName, entryStore, 0));
        IMap<String, String> map = instance.getMap(mapName);
        map.put("key", "value");
        // "value" is never stored in EntryStore because we do not grant a store permit
        map.evict("key");
        sleepAtLeastSeconds(2);
        // this value is retrieved from staging area
        assertEquals("value", map.get("key"));
    }

    @Test
    public void testEntryLoadedFromStagingAreaEnforcesNoExpirationTime_loadAll() {
        final String mapName = randomMapName();
        final int entryCount = 100;
        Map<String, String> localMap = new HashMap<>();
        for (int i = 0; i < entryCount; i++) {
            localMap.put("key" + i, "value" + i);
        }
        TemporaryBlockerEntryStore entryStore = new TemporaryBlockerEntryStore();
        HecatonInstance instance = createHecatonInstance(getConfig(mapName, entryStore, 0));
        IMap<String, String> map = instance.getMap(mapName);
        map.putAll(localMap);
        map.evictAll();
        sleepAtLeastSeconds(2);
        Map<String, String> stagedEntries = map.getAll(localMap.keySet());
        for (int i = 0; i < entryCount; i++) {
            assertEquals("value" + i, stagedEntries.get("key" + i));
        }
    }

    private Config getConfig(String mapName, EntryStore entryStore, int ttl) {
        Config config = new Config();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(entryStore).setEnabled(true);
        mapStoreConfig.setWriteDelaySeconds(1);
        config.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig).setTimeToLiveSeconds(ttl);
        return config;
    }
}
