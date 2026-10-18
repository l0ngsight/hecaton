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

package uk.org.hecaton.map.impl.nearcache;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.proxy.NearCachedMapProxyImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static uk.org.hecaton.internal.nearcache.impl.invalidation.RepairingTask.MAX_TOLERATED_MISS_COUNT;
import static uk.org.hecaton.internal.util.RandomPicker.getInt;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapNearCacheStalenessTest extends HecatonTestSupport {

    private static final int ENTRY_COUNT = 10;
    private static final int NEAR_CACHE_INVALIDATOR_THREAD_COUNT = 3;
    private static final int NEAR_CACHE_PUTTER_THREAD_COUNT = 10;
    private static final int NEAR_CACHE_REMOVER_THREAD_COUNT = 3;

    private final AtomicBoolean stop = new AtomicBoolean();

    private IMap<Integer, Integer> map1;
    private IMap<Integer, Integer> map2;

    @Before
    public void setUp() {
        String mapName = randomMapName();

        Config config = getConfig(mapName);

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);
        HecatonInstance node1 = factory.newHecatonInstance(config);
        HecatonInstance node2 = factory.newHecatonInstance(config);

        map1 = node1.getMap(mapName);
        map2 = node2.getMap(mapName);
    }

    @Test
    public void testNearCache_notContainsStaleValue_whenUpdatedByMultipleThreads() {
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < NEAR_CACHE_INVALIDATOR_THREAD_COUNT; i++) {
            Thread putter = new NearCacheInvalidator(stop, map1, ENTRY_COUNT);
            threads.add(putter);
        }
        for (int i = 0; i < NEAR_CACHE_PUTTER_THREAD_COUNT; i++) {
            Thread getter = new NearCachePutter(stop, map2, ENTRY_COUNT);
            threads.add(getter);
        }
        for (int i = 0; i < NEAR_CACHE_REMOVER_THREAD_COUNT; i++) {
            Thread remover = new NearCacheRemover(stop, map2, ENTRY_COUNT);
            threads.add(remover);
        }
        for (Thread thread : threads) {
            thread.start();
        }

        sleepSeconds(5);
        stop.set(true);
        for (Thread thread : threads) {
            assertJoinable(thread);
        }

        assertTrueEventually(() -> {
            assertNoStaleDataExistInNearCache(map1);
            assertNoStaleDataExistInNearCache(map2);
        });
    }

    protected Config getConfig(String mapName) {
        MapConfig mapConfig = getMapConfig(mapName);

        return getBaseConfig()
                .setProperty(MAX_TOLERATED_MISS_COUNT.getName(), "0")
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), "2")
                .addMapConfig(mapConfig);
    }

    protected MapConfig getMapConfig(String mapName) {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(mapName);

        return new MapConfig(mapName)
                    .setNearCacheConfig(nearCacheConfig);
    }

    protected NearCacheConfig getNearCacheConfig(String mapName) {
        return new NearCacheConfig(mapName)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setInvalidateOnChange(true)
                .setCacheLocalEntries(true);
    }

    private static void assertNoStaleDataExistInNearCache(IMap<Integer, Integer> map) {
        // 1. get all entries when Near Cache is full, so some values will come from Near Cache
        Map<Integer, Integer> fromNearCache = getAllEntries(map, ENTRY_COUNT);

        // 2. clear Near Cache
        ((NearCachedMapProxyImpl) map).getNearCache().clear();

        // 3. get all values when Near Cache is empty, these requests
        // will go directly to underlying IMap because Near Cache is empty
        Map<Integer, Integer> fromIMap = getAllEntries(map, ENTRY_COUNT);

        for (int i = 0; i < ENTRY_COUNT; i++) {
            assertEquals(fromIMap.get(i), fromNearCache.get(i));
        }
    }

    public static HashMap<Integer, Integer> getAllEntries(IMap<Integer, Integer> map, int entryCount) {
        HashMap<Integer, Integer> localMap = new HashMap<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            localMap.put(i, map.get(i));
        }
        return localMap;
    }

    private abstract static class NearCacheThread extends Thread {

        final AtomicBoolean stop;
        final IMap<Integer, Integer> map;
        final int entryCount;

        NearCacheThread(AtomicBoolean stop, IMap<Integer, Integer> map, int entryCount) {
            this.stop = stop;
            this.map = map;
            this.entryCount = entryCount;
        }
    }

    public static class NearCacheInvalidator extends NearCacheThread {

        public NearCacheInvalidator(AtomicBoolean stop, IMap<Integer, Integer> map, int entryCount) {
            super(stop, map, entryCount);
        }

        @Override
        public void run() {
            while (!stop.get()) {
                for (int i = 0; i < ENTRY_COUNT; i++) {
                    map.put(i, getInt(ENTRY_COUNT));
                }
            }
        }
    }

    public static class NearCachePutter extends NearCacheThread {

        public NearCachePutter(AtomicBoolean stop, IMap<Integer, Integer> map, int entryCount) {
            super(stop, map, entryCount);
        }

        @Override
        public void run() {
            while (!stop.get()) {
                for (int i = 0; i < ENTRY_COUNT; i++) {
                    map.get(i);
                }
            }
        }
    }

    public static class NearCacheRemover extends NearCacheThread {

        public NearCacheRemover(AtomicBoolean stop, IMap<Integer, Integer> map, int entryCount) {
            super(stop, map, entryCount);
        }

        @Override
        public void run() {
            while (!stop.get()) {
                for (int i = 0; i < ENTRY_COUNT; i++) {
                    map.remove(i);
                }
            }
        }
    }
}
