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

import uk.org.hecaton.client.map.helpers.AMapStore;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.config.XmlConfigBuilder;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.EntryProcessor;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.MapLoader;
import uk.org.hecaton.map.MapStore;
import uk.org.hecaton.map.ReachedMaxSizeException;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.mapstore.MapStoreTest;
import uk.org.hecaton.map.impl.proxy.MapProxyImpl;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category(SlowTest.class)
public class ClientMapStoreTest extends HecatonTestSupport {

    private static final String MAP_NAME = "clientMapStoreLoad";

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();
    private Config nodeConfig;

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Before
    public void setup() {
        nodeConfig = getConfig();
        MapConfig mapConfig = new MapConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new SimpleMapStore());
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapConfig.setName(MAP_NAME);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        nodeConfig.addMapConfig(mapConfig);
    }

    @Test
    public void testOneClient_KickOffMapStoreLoad() {
        hecatonFactory.newHecatonInstance(nodeConfig);

        ClientThread client1 = new ClientThread();
        client1.start();

        assertJoinable(client1);
        assertSizeEventually(SimpleMapStore.MAX_KEYS, client1.map);
    }

    @Test
    public void testTwoClient_KickOffMapStoreLoad() {
        hecatonFactory.newHecatonInstance(nodeConfig);
        ClientThread[] clientThreads = new ClientThread[2];
        for (int i = 0; i < clientThreads.length; i++) {
            ClientThread client1 = new ClientThread();
            client1.start();
            clientThreads[i] = client1;
        }

        assertJoinable(clientThreads);

        for (ClientThread c : clientThreads) {
            assertSizeEventually(SimpleMapStore.MAX_KEYS, c.map);
        }
    }

    @Test
    public void testOneClientKickOffMapStoreLoad_ThenNodeJoins() {
        hecatonFactory.newHecatonInstance(nodeConfig);

        ClientThread client1 = new ClientThread();
        client1.start();

        hecatonFactory.newHecatonInstance(nodeConfig);

        assertJoinable(client1);

        assertSizeEventually(SimpleMapStore.MAX_KEYS, client1.map);
    }

    @Test
    public void testForIssue2112() {
        hecatonFactory.newHecatonInstance(nodeConfig);
        IMap<String, String> map = hecatonFactory.newHecatonClient().getMap(ClientMapStoreTest.MAP_NAME);
        assertSizeEventually(SimpleMapStore.MAX_KEYS, map);
        hecatonFactory.newHecatonInstance(nodeConfig);
        map = hecatonFactory.newHecatonClient().getMap(ClientMapStoreTest.MAP_NAME);
        assertSizeEventually(SimpleMapStore.MAX_KEYS, map);
    }

    @Test
    public void mapSize_After_MapStore_OperationQueue_OverFlow() throws Exception {
        int maxCapacity = 1000;

        Config config = getConfig();
        config.setProperty(ClusterProperty.MAP_WRITE_BEHIND_QUEUE_CAPACITY.getName(), String.valueOf(maxCapacity));

        MapConfig mapConfig = new MapConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();

        MapStoreBackup store = new MapStoreBackup();
        int delaySeconds = Integer.MAX_VALUE;
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(store);
        mapStoreConfig.setWriteDelaySeconds(delaySeconds);
        mapStoreConfig.setWriteCoalescing(false);

        mapConfig.setName(MAP_NAME);

        mapConfig.setMapStoreConfig(mapStoreConfig);
        config.addMapConfig(mapConfig);

        HecatonInstance server = hecatonFactory.newHecatonInstance(config);
        HecatonInstance client = hecatonFactory.newHecatonClient();
        IMap<Integer, Integer> map = client.getMap(MAP_NAME);

        int overflow = 100;
        List<Future> futures = new ArrayList<>(maxCapacity + overflow);
        for (int i = 0; i < maxCapacity + overflow; i++) {
            Future future = map.putAsync(i, i).toCompletableFuture();
            futures.add(future);
        }

        int success = 0;
        for (Future future : futures) {
            try {
                future.get();
                success++;
            } catch (ExecutionException e) {
                assertInstanceOf(ReachedMaxSizeException.class, e.getCause());
            }
        }

        assertEquals(maxCapacity, success);
        assertEquals(maxCapacity, map.size());
    }

    @Test
    public void mapStore_OperationQueue_AtMaxCapacity() {
        int maxCapacity = 1000;

        Config config = getConfig();
        config.setProperty(ClusterProperty.MAP_WRITE_BEHIND_QUEUE_CAPACITY.getName(), String.valueOf(maxCapacity));

        MapConfig mapConfig = new MapConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();

        MapStoreBackup store = new MapStoreBackup();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(store);
        mapStoreConfig.setWriteDelaySeconds(60);
        mapStoreConfig.setWriteCoalescing(false);

        mapConfig.setName(MAP_NAME);

        mapConfig.setMapStoreConfig(mapStoreConfig);
        config.addMapConfig(mapConfig);

        HecatonInstance server = hecatonFactory.newHecatonInstance(config);
        HecatonInstance client = hecatonFactory.newHecatonClient();

        IMap<Integer, Integer> map = client.getMap(MAP_NAME);

        for (int i = 0; i < maxCapacity; i++) {
            map.put(i, i);
        }
        assertEquals(maxCapacity, map.size());

        try {
            map.put(maxCapacity, maxCapacity);
            fail("Should not exceed max capacity");
        } catch (ReachedMaxSizeException expected) {
            ignore(expected);
        }
    }

    @Test
    public void destroyMap_configuredWithMapStore() {
        Config config = getConfig();
        MapConfig mapConfig = new MapConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();

        MapStoreBackup store = new MapStoreBackup();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(store);
        mapStoreConfig.setWriteDelaySeconds(4);

        mapConfig.setName(MAP_NAME);

        mapConfig.setMapStoreConfig(mapStoreConfig);
        config.addMapConfig(mapConfig);

        HecatonInstance server = hecatonFactory.newHecatonInstance(config);
        HecatonInstance client = hecatonFactory.newHecatonClient();

        IMap<Integer, Integer> map = client.getMap(MAP_NAME);
        for (int i = 0; i < 1; i++) {
            map.putAsync(i, i);
        }

        map.destroy();
    }

    @Test
    public void testNonExistentEntriesValueAreNotInsertedIntoMap() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(new MapStoreTest.NullLoader());

        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(MAP_NAME);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        Config config = getConfig();
        config.addMapConfig(mapConfig);

        HecatonInstance server = hecatonFactory.newHecatonInstance(config);
        HecatonInstance client = hecatonFactory.newHecatonClient();
        HecatonInstance node = createHecatonInstance(config);
        IMap<String, String> map = node.getMap(MAP_NAME);

        //load not existent entries
        Map<String, String> responseMap = map.getAll(Set.of("key1", "key2", "key3"));
        assertEquals(0, responseMap.size());
        assertEquals(0, map.size());
    }


    @Test
    public void test_executeOnEntries_with_read_only_entry_processor() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(new SimpleMapStore());

        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(MAP_NAME);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        Config config = getConfig();
        config.addMapConfig(mapConfig);

        HecatonInstance server = hecatonFactory.newHecatonInstance(config);
        HecatonInstance client = hecatonFactory.newHecatonClient();

        IMap map = client.getMap(MAP_NAME);
        map.loadAll(true);

        int mapSize = map.size();

        Map responses = map.executeOnEntries(new CountItemsEP());

        assertEquals(mapSize, responses.size());
    }

    public static class CountItemsEP implements EntryProcessor {

        @Override
        public Object process(Map.Entry entry) {
            return 1;
        }
    }

    static class SimpleMapStore implements MapStore<String, String>, MapLoader<String, String> {

        static final int MAX_KEYS = 30;
        static final int DELAY_SECONDS_PER_KEY = 1;

        @Override
        public String load(String key) {
            sleepSeconds(DELAY_SECONDS_PER_KEY);
            return key + "value";
        }

        @Override
        public Map<String, String> loadAll(Collection<String> keys) {
            Map<String, String> map = new HashMap<>();
            for (String key : keys) {
                map.put(key, load(key));
            }
            return map;
        }

        @Override
        public Set<String> loadAllKeys() {
            Set<String> keys = new HashSet<>();

            for (int i = 0; i < MAX_KEYS; i++) {
                keys.add("key" + i);
            }

            return keys;
        }

        @Override
        public void delete(String key) {
            sleepSeconds(DELAY_SECONDS_PER_KEY);
        }

        @Override
        public void deleteAll(Collection<String> keys) {
            for (String key : keys) {
                delete(key);
            }
        }

        @Override
        public void store(String key, String value) {
            sleepSeconds(DELAY_SECONDS_PER_KEY);
        }

        @Override
        public void storeAll(Map<String, String> entries) {
            for (Map.Entry<String, String> e : entries.entrySet()) {
                store(e.getKey(), e.getValue());
            }
        }
    }

    private class ClientThread extends Thread {

        IMap<String, String> map;

        @Override
        public void run() {
            HecatonInstance client = hecatonFactory.newHecatonClient();
            map = client.getMap(ClientMapStoreTest.MAP_NAME);
            map.size();
        }
    }

    public class MapStoreBackup implements MapStore<Object, Object> {

        public final Map<Object, Object> store = new ConcurrentHashMap<>();

        @Override
        public void store(Object key, Object value) {
            store.put(key, value);
        }

        @Override
        public void storeAll(Map<Object, Object> map) {
            for (Map.Entry<Object, Object> kvp : map.entrySet()) {
                store.put(kvp.getKey(), kvp.getValue());
            }
        }

        @Override
        public void delete(Object key) {
            store.remove(key);
        }

        @Override
        public void deleteAll(Collection<Object> keys) {
            for (Object key : keys) {
                store.remove(key);
            }
        }

        @Override
        public Object load(Object key) {
            return store.get(key);
        }

        @Override
        public Map<Object, Object> loadAll(Collection<Object> keys) {
            Map<Object, Object> result = new HashMap<>();
            for (Object key : keys) {
                Object v = store.get(key);
                if (v != null) {
                    result.put(key, v);
                }
            }
            return result;
        }

        @Override
        public Set<Object> loadAllKeys() {
            return store.keySet();
        }
    }

    @Test
    public void testIssue3023_testWithSubStringMapNames() {
        String mapNameWithStore = "MapStore*";
        String mapNameWithStoreAndSize = "MapStoreMaxSize*";

        String xml = "<hecaton xsi:schemaLocation=\"https://hecaton.org.uk/schema/config\n"
                + "                             https://hecaton.org.uk/schema/config/hecaton-config-5.6.xsd\"\n"
                + "                             xmlns=\"https://hecaton.org.uk/schema/config\"\n"
                + "                             xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n"
                + "\n"
                + "    <map name=\"" + mapNameWithStore + "\">\n"
                + "        <map-store enabled=\"true\">\n"
                + "            <class-name>com.will.cause.problem.if.used</class-name>\n"
                + "            <write-delay-seconds>5</write-delay-seconds>\n"
                + "        </map-store>\n"
                + "    </map>\n"
                + "\n"
                + "    <map name=\"" + mapNameWithStoreAndSize + "\">\n"
                + "        <in-memory-format>BINARY</in-memory-format>\n"
                + "        <backup-count>1</backup-count>\n"
                + "        <async-backup-count>0</async-backup-count>\n"
                + "        <max-idle-seconds>0</max-idle-seconds>\n"
                + "        <eviction size=\"10\" max-size-policy=\"PER_NODE\" eviction-policy=\"LRU\" />\n"
                + "        <merge-policy>PassThroughMergePolicy</merge-policy>\n"
                + "        <map-store enabled=\"true\">\n"
                + "            <class-name>uk.org.hecaton.client.map.helpers.AMapStore</class-name>\n"
                + "            <write-delay-seconds>5</write-delay-seconds>\n"
                + "        </map-store>\n"
                + "    </map>\n"
                + "\n"
                + "</hecaton>";

        Config config = buildConfig(xml);
        HecatonInstance hz = hecatonFactory.newHecatonInstance(config);
        HecatonInstance client = hecatonFactory.newHecatonClient();

        IMap<Integer, Integer> map = client.getMap(mapNameWithStoreAndSize + "1");
        map.put(1, 1);

        final AMapStore store = getMapStoreInstance(hz, mapNameWithStoreAndSize + "1");

        assertTrueEventually(() -> assertEquals(1, store.store.get(1)));
    }

    private Config buildConfig(String xml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        return configBuilder.build();
    }

    // given a member-side HecatonInstance and a mapName, return the MapStore instance
    // or null if none configured
    private <T extends MapStore> T getMapStoreInstance(HecatonInstance hz, String mapName) {
        MapProxyImpl map = (MapProxyImpl) hz.getMap(mapName);
        MapService service = (MapService) map.getService();
        MapServiceContext context = service.getMapServiceContext();
        MapContainer container = context.getMapContainer(mapName);
        return (T) container.getMapStoreContext().getMapStoreWrapper().getMapStore();
    }
}
