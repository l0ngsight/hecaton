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
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static uk.org.hecaton.config.MapStoreConfig.InitialLoadMode.LAZY;
import static uk.org.hecaton.test.TimeConstants.MINUTE;
import static org.junit.Assert.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapLoaderFailoverTest extends HecatonTestSupport {

    @Rule
    public Timeout timeout = new Timeout(MINUTE, TimeUnit.MILLISECONDS);

    private static final int MAP_STORE_ENTRY_COUNT = 10000;
    private static final int BATCH_SIZE = 100;
    private static final int NODE_COUNT = 3;
    protected static final String MAP_NAME = "default";

    private TestHecatonInstanceFactory nodeFactory;
    private CountingMapLoader mapLoader;

    @Before
    public void setUp() {
        assumeNotZing();
        nodeFactory = createHecatonInstanceFactory(NODE_COUNT + 2);
        mapLoader = new CountingMapLoader(MAP_STORE_ENTRY_COUNT);
    }

    @Test
    public void testDoesNotLoadAgain_whenLoaderNodeGoesDown() {
        HecatonInstance[] nodes
                = nodeFactory.newInstances(this::newConfig, 3);
        HecatonInstance hz3 = nodes[2];

        String mapName = generateKeyOwnedBy(hz3);
        IMap<Object, Object> map = nodes[0].getMap(mapName);

        assertSizeAndLoadCount(map);

        hz3.getLifecycleService().terminate();
        waitAllForSafeState(nodeFactory.getAllHecatonInstances());

        assertSizeAndLoadCount(map);
        assertEquals(1, mapLoader.getLoadAllKeysInvocations());
    }

    @Test
    public void testLoads_whenInitialLoaderNodeRemoved() {
        HecatonInstance[] nodes
                = nodeFactory.newInstances(this::newConfig, 3);
        HecatonInstance hz3 = nodes[2];

        String mapName = generateKeyOwnedBy(hz3);
        IMap<Object, Object> map = nodes[0].getMap(mapName);
        hz3.getLifecycleService().terminate();

        // trigger loading
        map.size();

        assertEquals(1, mapLoader.getLoadAllKeysInvocations());
        assertSizeAndLoadCount(map);
    }

    /** @see <a href="https://github.com/hazelcast/hazelcast/issues/6056">Fixes</a> */
    @Test
    public void testLoadsAll_whenInitialLoaderNodeRemovedAfterLoading() {
        HecatonInstance[] nodes
                = nodeFactory.newInstances(this::newConfig, 3);
        HecatonInstance hz3 = nodes[2];

        String mapName = generateKeyOwnedBy(hz3);
        IMap<Object, Object> map = nodes[0].getMap(mapName);

        assertSizeAndLoadCount(map);

        hz3.getLifecycleService().terminate();
        assertClusterSizeEventually(2, nodes[0]);

        map.loadAll(true);

        assertSizeEventually(MAP_STORE_ENTRY_COUNT, map);
        assertEquals(2, mapLoader.getLoadAllKeysInvocations());
        assertEquals(2 * MAP_STORE_ENTRY_COUNT, mapLoader.getLoadedValueCount());
    }

    @Test
    public void testLoadsAll_whenInitialLoaderNodeRemovedWhileLoading() throws Exception {
        PausingMapLoader<Integer, Integer> pausingLoader = new PausingMapLoader<>(mapLoader, 5000);
        HecatonInstance[] nodes
                = nodeFactory.newInstances(() -> newConfig(1, pausingLoader), 3);
        HecatonInstance hz3 = nodes[2];

        String mapName = generateKeyOwnedBy(hz3);
        IMap<Integer, Integer> map = nodes[0].getMap(mapName);

        // trigger loading and pause halfway through
        Future<Integer> asyncVal = map.getAsync(1).toCompletableFuture();
        pausingLoader.awaitPause();

        hz3.getLifecycleService().terminate();
        assertClusterSizeEventually(2, nodes[0]);

        pausingLoader.resume();

        // workaround for a known MapLoader issue documented in #12384
        //
        // in short, there is an edge case in which the get operation is
        // processed before loading the partition holding the given key
        // restarts on the previous replica node, after the owner node
        // died during the load process
        // for the details, see the issue
        //
        // we do this workaround since the goal of the test is to verify
        // that loadAll() eventually loads all records even if a node
        // dies in the middle of loading
        AtomicReference<Integer> resultRef = new AtomicReference<>(asyncVal.get());
        assertEqualsEventually(() -> {
            if (resultRef.get() == null) {
                resultRef.set(map.get(1));
            }

            return resultRef.get();
        }, 1);

        assertSizeEventually(MAP_STORE_ENTRY_COUNT, map);
        assertThat(mapLoader.getLoadedValueCount()).isGreaterThanOrEqualTo(MAP_STORE_ENTRY_COUNT);
        assertEquals("uk.org.hecaton.map.impl.mapstore.CountingMapLoader#loadAllKeys() called an unexpected number of times", 2,
                mapLoader.getLoadAllKeysInvocations());
    }

    /** @see <a href="https://github.com/hazelcast/hazelcast/issues/7959">Fixes</a> */
    @Test
    public void testLoadsAll_whenInitialLoaderNodeRemovedWhileLoadingAndNoBackups() {
        CountingMapLoader mapLoader1 = new CountingMapLoader(MAP_STORE_ENTRY_COUNT);
        CountingMapLoader mapLoader2 = new CountingMapLoader(MAP_STORE_ENTRY_COUNT);
        PausingMapLoader<Integer, Integer> pausingLoader3
                = new PausingMapLoader<>(new CountingMapLoader(MAP_STORE_ENTRY_COUNT), 5000);

        Config cfg1 = newConfig(0, mapLoader1);
        Config cfg2 = newConfig(0, mapLoader2);
        Config cfg3 = newConfig(0, pausingLoader3);

        HecatonInstance node1 = nodeFactory.newHecatonInstance(cfg1);
        nodeFactory.newHecatonInstance(cfg2);
        HecatonInstance node3 = nodeFactory.newHecatonInstance(cfg3);

        String mapName = generateKeyOwnedBy(node3);
        IMap<Object, Object> map = node1.getMap(mapName);

        // trigger loading and pause halfway through
        map.putAsync(1, 2);
        pausingLoader3.awaitPause();

        node3.getLifecycleService().terminate();
        waitAllForSafeState(nodeFactory.getAllHecatonInstances());

        pausingLoader3.resume();

        assertEquals(MAP_STORE_ENTRY_COUNT, map.size());

        int loadedValueCount = mapLoader1.getLoadedValueCount() + mapLoader2.getLoadedValueCount();
        assertThat(loadedValueCount).isGreaterThanOrEqualTo(MAP_STORE_ENTRY_COUNT);

        assertEquals(1, mapLoader1.getLoadAllKeysInvocations()
                + mapLoader2.getLoadAllKeysInvocations());
    }

    private void assertSizeAndLoadCount(IMap<?, ?> map) {
        assertSizeEventually(MAP_STORE_ENTRY_COUNT, map);
        assertEquals(MAP_STORE_ENTRY_COUNT, mapLoader.getLoadedValueCount());
    }

    private Config newConfig() {
        return newConfig(1, mapLoader);
    }

    protected Config newConfig(int backups, MapLoader<?, ?> loader) {
        Config config = smallInstanceConfigWithoutJetAndMetrics().setClusterName(getClass().getSimpleName())
                .setProperty(ClusterProperty.MAP_LOAD_CHUNK_SIZE.getName(), Integer.toString(BATCH_SIZE))
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "13");

        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setInitialLoadMode(LAZY)
                .setImplementation(loader);

        config.getMapConfig(MAP_NAME)
                .setBackupCount(backups)
                .setMapStoreConfig(mapStoreConfig);

        return config;
    }
}
