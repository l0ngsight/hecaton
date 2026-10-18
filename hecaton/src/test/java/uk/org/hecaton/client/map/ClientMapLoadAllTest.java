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

package uk.org.hecaton.client.map;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.MapStore;
import uk.org.hecaton.map.impl.mapstore.AbstractMapStoreTest;
import uk.org.hecaton.map.impl.mapstore.MapLoaderTest;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.test.TestCollectionUtils.setOfValuesBetween;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapLoadAllTest extends AbstractMapStoreTest {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testGetMap_issue_3031() {
        final int itemCount = 1000;
        final String mapName = randomMapName();

        final AtomicBoolean breakMe = new AtomicBoolean();

        final Config config = createNewConfig(mapName, new BrokenLoadSimpleStore(breakMe));
        final HecatonInstance server = hecatonFactory.newHecatonInstance(config);
        hecatonFactory.newHecatonInstance(config);
        hecatonFactory.newHecatonInstance(config);

        try {
            final IMap<Object, Object> map = server.getMap(mapName);
            populateMap(map, itemCount);
            map.clear();
        } catch (Exception e) {
            fail();
        }

        breakMe.set(true);

        final HecatonInstance client = hecatonFactory.newHecatonClient(getClientConfig());
        client.getMap(mapName);
    }

    @Test
    public void testLoadAll_givenKeys() {
        final String mapName = randomMapName();
        final Config config = createNewConfig(mapName);
        hecatonFactory.newHecatonInstance(config);
        hecatonFactory.newHecatonInstance(config);
        hecatonFactory.newHecatonInstance(config);

        final HecatonInstance client = hecatonFactory.newHecatonClient(getClientConfig());
        final IMap<Object, Object> map = client.getMap(mapName);
        populateMap(map, 1000);
        map.evictAll();
        final Set keysToLoad = selectKeysToLoad(10, 910);
        map.loadAll(keysToLoad, true);

        assertEquals(900, map.size());
        assertRangeLoaded(map, 10, 910);
    }

    @Test
    public void givenSpecificKeysWereReloaded_whenLoadAllIsCalled_thenAllEntriesAreLoadedFromTheStore() {
        String name = randomString();
        int keysInMapStore = 10000;

        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig(name);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new MapLoaderTest.DummyMapLoader(keysInMapStore));
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        hecatonFactory.newHecatonInstance(config);

        HecatonInstance client = hecatonFactory.newHecatonClient(getClientConfig());
        IMap<Integer, Integer> map = client.getMap(name);

        //load specific keys
        map.loadAll(setOfValuesBetween(0, keysInMapStore), true);

        //remove everything
        map.clear();

        //assert loadAll with load all entries provided by the mapLoader
        map.loadAll(true);
        assertEquals(keysInMapStore, map.size());
    }

    @Test
    public void testLoadAll_allKeys() {
        final String mapName = randomMapName();
        final Config config = createNewConfig(mapName);
        hecatonFactory.newHecatonInstance(config);
        hecatonFactory.newHecatonInstance(config);
        hecatonFactory.newHecatonInstance(config);
        final HecatonInstance client = hecatonFactory.newHecatonClient(getClientConfig());
        final IMap<Object, Object> map = client.getMap(mapName);
        populateMap(map, 1000);
        map.evictAll();
        map.loadAll(true);

        assertEquals(1000, map.size());
    }

    protected ClientConfig getClientConfig() {
        return new ClientConfig();
    }

    private Config createNewConfig(String mapName) {
        return createNewConfig(mapName, new SimpleStore());
    }

    private Config createNewConfig(String mapName, MapStore mapStore) {
        return newConfig(mapName, mapStore, 0);
    }

    private static void populateMap(IMap map, int itemCount) {
        for (int i = 0; i < itemCount; i++) {
            map.put(i, i);
        }
    }

    private static Set selectKeysToLoad(int rangeStart, int rangeEnd) {
        final Set keysToLoad = new HashSet();
        for (int i = rangeStart; i < rangeEnd; i++) {
            keysToLoad.add(i);
        }
        return keysToLoad;
    }

    private static void assertRangeLoaded(IMap map, int rangeStart, int rangeEnd) {
        for (int i = rangeStart; i < rangeEnd; i++) {
            assertEquals(i, map.get(i));
        }
    }

    private static class SimpleStore implements MapStore {

        private ConcurrentMap store = new ConcurrentHashMap();

        @Override
        public void store(Object key, Object value) {
            store.put(key, value);
        }

        @Override
        public void storeAll(Map map) {
            final Set<Map.Entry> entrySet = map.entrySet();
            for (Map.Entry entry : entrySet) {
                final Object key = entry.getKey();
                final Object value = entry.getValue();
                store(key, value);
            }
        }

        @Override
        public void delete(Object key) {
        }

        @Override
        public void deleteAll(Collection keys) {
        }

        @Override
        public Object load(Object key) {
            return store.get(key);
        }

        @Override
        public Map loadAll(Collection keys) {
            final Map map = new HashMap();
            for (Object key : keys) {
                final Object value = load(key);
                map.put(key, value);
            }
            return map;
        }

        @Override
        public Set loadAllKeys() {
            return store.keySet();
        }
    }

    private static class BrokenLoadSimpleStore extends SimpleStore {

        private final AtomicBoolean breakMe;

        private BrokenLoadSimpleStore(AtomicBoolean breakMe) {
            this.breakMe = breakMe;
        }

        @Override
        public Object load(Object key) {
            testClientRequest();
            return null;
        }

        @Override
        public Map loadAll(Collection keys) {
            testClientRequest();
            return null;
        }

        private boolean testClientRequest() {
            if (breakMe.get()) {
                throw new ClassCastException();
            }
            return false;
        }
    }
}
