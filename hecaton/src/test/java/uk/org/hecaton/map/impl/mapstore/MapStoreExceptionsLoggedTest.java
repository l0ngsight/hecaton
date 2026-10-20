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

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.MapLoaderLifecycleSupport;
import uk.org.hecaton.map.MapStore;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.ExceptionRecorder;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that exceptions thrown from MapStore/Loader are logged.
 * It takes some time to run with all the combinations, so it's {@link NightlyTest}.
 */
@RunWith(HecatonParametrizedRunner.class)
@Category({NightlyTest.class})
public class MapStoreExceptionsLoggedTest extends HecatonTestSupport {

    private static ILogger log = Logger.getLogger(MapStoreExceptionsLoggedTest.class);

    private TestHecatonFactory factory;

    @Parameter
    public boolean offload;

    @Parameter(1)
    public boolean useClient;

    @Parameter(2)
    public int memberCount;

    @Parameters(name = "offload={0}, useClient={1}, memberCount={2}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true, true, 1},
                {true, true, 2},
                {true, false, 1},
                {true, false, 2},
                {false, true, 1},
                {false, true, 2},
                {false, false, 1},
                {false, false, 2}
        });
    }

    @Before
    public void setUp() throws Exception {
        factory = new TestHecatonFactory();
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test
    public void when_init_throws_exception_it_should_be_logged() {
        run(
                (mapStore) -> mapStore.withInitException(() -> new RuntimeException("init exception")),
                (map) -> map.loadAll(false),
                "init exception"
        );
    }

    @Test
    public void when_destroy_throws_exception_it_should_be_logged() {
        run(
                (mapStore) -> mapStore.withDestroyException(() -> new RuntimeException("destroy exception")),
                (map) -> {
                    map.loadAll(false);
                    map.destroy();
                },
                "destroy exception"
        );
    }

    @Test
    public void when_loadAllKeys_throws_exception_it_should_be_logged() {
        run(
                (mapStore) -> mapStore.withLoadAllKeysException(() -> new RuntimeException("loadAllKeys exception")),
                (map) -> map.loadAll(true),
                "loadAllKeys exception"
        );
    }

    @Test
    public void when_loadAll_false_throws_exception_it_should_be_logged() {
        run(
                (mapStore) -> mapStore.withLoadAllException(() -> new RuntimeException("loadAll exception")),
                (map) -> map.loadAll(false),
                "loadAll exception"
        );
    }

    @Test
    public void when_loadAll_true_throws_exception_it_should_be_logged() {
        run(
                (mapStore) -> mapStore.withLoadAllException(() -> new RuntimeException("loadAll exception")),
                (map) -> map.loadAll(true),
                "loadAll exception"
        );
    }

    @Test
    public void when_load_throws_exception_it_should_be_logged() {
        run(
                (mapStore) -> mapStore.withLoadException(() -> new RuntimeException("load exception")),
                (map) -> map.get(-1),
                "load exception"
        );
    }

    @Test
    public void when_store_throws_exception_it_should_be_logged() {
        run(
                (mapStore) -> mapStore.withStoreException(() -> new RuntimeException("store exception")),
                (map) -> map.put(4, "d"),
                "store exception"
        );
    }

    @Test
    public void when_storeAll_throws_exception_it_should_be_logged() {
        // This test takes between 3-5 seconds:
        // up to 1 s waiting for the batch of items
        // the call to storeAll fails, there are 3 retries after 1s, unfortunately it's not configurable
        run(
                (mapStore) -> mapStore.withStoreAllException(() -> new RuntimeException("storeAll exception")),
                (mapStoreConfig) -> mapStoreConfig.setWriteDelaySeconds(1),
                (map) -> {
                    // Multiple items so the operations runs across both members
                    Map<Integer, String> update = new HashMap<>();
                    for (int i = 0; i < 100; i++) {
                        update.put(i, String.valueOf(i + 1));
                    }
                    map.putAll(update);
                },
                "storeAll exception"
        );
    }

    @Test
    public void when_delete_throws_exception_it_should_be_logged() {
        run(
                (mapStore) -> mapStore.withDeleteException(() -> new RuntimeException("delete exception")),
                (map) -> map.remove(1),
                "delete exception"
        );
    }

    @Test
    public void when_deleteAll_throws_exception_it_should_be_logged() {
        run(
                (mapStore) -> mapStore.withDeleteAllException(() -> new RuntimeException("deleteAll exception")),
                (mapStoreConfig) -> mapStoreConfig.setWriteDelaySeconds(1),
                (map) -> {
                    // Multiple items so the operations runs across both members
                    for (int i = 0; i < 100; i++) {
                        map.remove(i);
                    }
                },
                "deleteAll exception"
        );
    }

    private void run(
            Consumer<ThrowingMapStore> setupMapStore,
            Consumer<IMap<Integer, String>> mapOperation,
            String expectedMessage
    ) {
        run(setupMapStore, (mapStoreConfig) -> {
        }, mapOperation, expectedMessage);
    }

    private void run(
            Consumer<ThrowingMapStore> setupMapStore,
            Consumer<MapStoreConfig> setupMapStoreConfig,
            Consumer<IMap<Integer, String>> mapOperation,
            String expectedMessage
    ) {
        ThrowingMapStore mapStore = new ThrowingMapStore();
        setupMapStore.accept(mapStore);

        MapConfig mapConfig = new MapConfig("my-map");
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setOffload(offload)
                .setImplementation(mapStore);
        setupMapStoreConfig.accept(mapStoreConfig);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        Config config = smallInstanceConfig()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "3")
                .addMapConfig(mapConfig);

        HecatonInstance[] members = factory.newInstances(config, memberCount);
        HecatonInstance instance;
        if (useClient) {
            instance = factory.newHecatonClient();
        } else {
            instance = members[0];
        }

        ExceptionRecorder recorder = new ExceptionRecorder(members, Level.INFO);

        try {
            mapOperation.accept(instance.getMap("my-map"));
        } catch (Exception e) {
            // In this test we don't care about what the operations return back to the caller,
            // or if they throw, we care only about the log
            log.info("Operation threw an exception", e);
        }

        assertTrueEventually(
                () -> assertThat(recorder.exceptionsLogged())
                        .map(Throwable::getMessage)
                        .contains(expectedMessage),
                10
        );
    }

    static class ThrowingMapStore implements MapStore<Integer, String>, MapLoaderLifecycleSupport {

        private final Map<Integer, String> data = new ConcurrentHashMap<>();

        {
            for (int i = 0; i < 100; i++) {
                data.put(i, String.valueOf(i));
            }
        }

        // Using suppliers, if we used plain field the stacktrace would point to initialization of this class
        private Supplier<RuntimeException> initException;
        private Supplier<RuntimeException> destroyException;
        private Supplier<RuntimeException> loadAllKeysException;
        private Supplier<RuntimeException> loadAllException;
        private Supplier<RuntimeException> loadException;
        private Supplier<RuntimeException> storeException;
        private Supplier<RuntimeException> storeAllException;
        private Supplier<RuntimeException> deleteException;
        private Supplier<RuntimeException> deleteAllException;

        public ThrowingMapStore withInitException(Supplier<RuntimeException> e) {
            this.initException = e;
            return this;
        }

        public ThrowingMapStore withDestroyException(Supplier<RuntimeException> e) {
            this.destroyException = e;
            return this;
        }

        public ThrowingMapStore withLoadAllKeysException(Supplier<RuntimeException> e) {
            this.loadAllKeysException = e;
            return this;
        }

        public ThrowingMapStore withLoadAllException(Supplier<RuntimeException> e) {
            this.loadAllException = e;
            return this;
        }

        public ThrowingMapStore withLoadException(Supplier<RuntimeException> e) {
            this.loadException = e;
            return this;
        }

        public ThrowingMapStore withStoreException(Supplier<RuntimeException> e) {
            this.storeException = e;
            return this;
        }

        public ThrowingMapStore withStoreAllException(Supplier<RuntimeException> e) {
            this.storeAllException = e;
            return this;
        }

        public ThrowingMapStore withDeleteException(Supplier<RuntimeException> e) {
            this.deleteException = e;
            return this;
        }

        public ThrowingMapStore withDeleteAllException(Supplier<RuntimeException> e) {
            this.deleteAllException = e;
            return this;
        }

        @Override
        public void init(HecatonInstance hecatonInstance, Properties properties, String mapName) {
            if (initException != null) {
                throw initException.get();
            }
        }

        @Override
        public void destroy() {
            if (destroyException != null) {
                throw destroyException.get();
            }
        }

        @Override
        public String load(Integer key) {
            if (loadException != null) {
                throw loadException.get();
            } else {
                return data.get(key);
            }
        }

        @Override
        public Map<Integer, String> loadAll(Collection<Integer> keys) {
            if (loadAllException != null) {
                throw loadAllException.get();
            } else {
                return keys.stream()
                           .map(k -> new SimpleEntry<>(k, data.get(k)))
                           .collect(toMap(SimpleEntry::getKey, SimpleEntry::getValue));
            }
        }

        @Override
        public Iterable<Integer> loadAllKeys() {
            if (loadAllKeysException != null) {
                throw loadAllKeysException.get();
            } else {
                return data.keySet();
            }
        }

        @Override
        public void store(Integer key, String value) {
            if (storeException != null) {
                throw storeException.get();
            } else {
                data.put(key, value);
            }
        }

        @Override
        public void storeAll(Map<Integer, String> map) {
            if (storeAllException != null) {
                throw storeAllException.get();
            } else {
                data.putAll(map);
            }
        }

        @Override
        public void delete(Integer key) {
            if (deleteException != null) {
                throw deleteException.get();
            } else {
                data.remove(key);
            }
        }

        @Override
        public void deleteAll(Collection<Integer> keys) {
            if (deleteAllException != null) {
                throw deleteAllException.get();
            } else {
                for (Integer key : keys) {
                    data.remove(key);
                }
            }
        }
    }
}
