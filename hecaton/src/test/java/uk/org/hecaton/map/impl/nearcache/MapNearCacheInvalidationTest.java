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
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.nearcache.NearCache;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.impl.proxy.NearCachedMapProxyImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static uk.org.hecaton.config.InMemoryFormat.OBJECT;
import static uk.org.hecaton.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static uk.org.hecaton.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;
import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_COUNT;
import static java.lang.String.valueOf;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapNearCacheInvalidationTest extends HecatonTestSupport {

    private final String mapName = randomMapName();

    @Test
    public void testBatchInvalidationRemovesEntries() {
        Config config = getConfig(mapName)
                .setProperty(PARTITION_COUNT.getName(), "1");
        configureBatching(config, 12, 1);

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance node1 = factory.newHecatonInstance(config);
        HecatonInstance node2 = factory.newHecatonInstance(config);

        final IMap<Integer, Integer> map1 = node1.getMap(mapName);
        final IMap<Integer, Integer> map2 = node2.getMap(mapName);

        int size = 1000;

        // fill map-1
        for (int i = 0; i < size; i++) {
            map1.put(i, i);
        }

        // fill Near Cache on node-1
        for (int i = 0; i < size; i++) {
            map1.get(i);
        }

        // fill Near Cache on node-2
        for (int i = 0; i < size; i++) {
            map2.get(i);
        }

        // generate invalidation data
        for (int i = 0; i < size; i++) {
            map1.put(i, i);
        }

        assertTrueEventually(() -> {
            NearCache nearCache1 = ((NearCachedMapProxyImpl) map1).getNearCache();
            NearCache nearCache2 = ((NearCachedMapProxyImpl) map2).getNearCache();
            assertEquals(0, nearCache1.size() + nearCache2.size());
        });
    }

    @Test
    public void testHigherBatchSize_shouldNotCauseAnyInvalidation_onRemoteNode() {
        Config config = getConfig(mapName);
        configureBatching(config, Integer.MAX_VALUE, Integer.MAX_VALUE);

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance node1 = factory.newHecatonInstance(config);
        HecatonInstance node2 = factory.newHecatonInstance(config);

        IMap<String, Integer> map1 = node1.getMap(mapName);
        IMap<String, Integer> map2 = node2.getMap(mapName);

        int size = 1000;

        List<String> keys = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            keys.add(generateKeyOwnedBy(node1));
        }

        // fill map-1
        for (int i = 0; i < size; i++) {
            map1.put(keys.get(i), i);
        }

        // fill Near Cache on node-1
        for (int i = 0; i < size; i++) {
            map1.get(keys.get(i));
        }

        // fill Near Cache on node-2
        for (int i = 0; i < size; i++) {
            map2.get(keys.get(i));
        }

        // generate invalidation data
        for (int i = 0; i < size; i++) {
            map1.put(keys.get(i), i);
        }

        NearCache nearCache1 = ((NearCachedMapProxyImpl) map1).getNearCache();
        NearCache nearCache2 = ((NearCachedMapProxyImpl) map2).getNearCache();

        // Near Cache on one node should be invalidated completely, other node should not receive any event
        // (due to the higher invalidation batch-size)
        assertEquals(size, nearCache1.size() + nearCache2.size());
    }

    @Test
    public void testMapClear_shouldClearNearCaches_onOwnerAndBackupNodes() {
        Config config = getConfig(mapName)
                .setProperty(PARTITION_COUNT.getName(), "1");
        configureBatching(config, 5, 5);

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance node1 = factory.newHecatonInstance(config);
        HecatonInstance node2 = factory.newHecatonInstance(config);

        final IMap<Integer, Integer> map1 = node1.getMap(mapName);
        final IMap<Integer, Integer> map2 = node2.getMap(mapName);

        int size = 1000;

        // fill map-1
        for (int i = 0; i < size; i++) {
            map1.put(i, i);
        }

        // fill Near Cache on node-1
        for (int i = 0; i < size; i++) {
            map1.get(i);
        }

        // fill Near Cache on node-2
        for (int i = 0; i < size; i++) {
            map2.get(i);
        }

        map1.clear();

        assertTrueEventually(() -> {
            NearCache nearCache1 = ((NearCachedMapProxyImpl) map1).getNearCache();
            NearCache nearCache2 = ((NearCachedMapProxyImpl) map2).getNearCache();
            assertEquals(0, nearCache1.size() + nearCache2.size());
        });
    }

    @Test
    public void testMapEvictAll_shouldClearNearCaches_onOwnerAndBackupNodes() {
        Config config = getConfig(mapName)
                .setProperty(PARTITION_COUNT.getName(), "1");
        configureBatching(config, 5, 5);

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance node1 = factory.newHecatonInstance(config);
        HecatonInstance node2 = factory.newHecatonInstance(config);

        final IMap<Integer, Integer> map1 = node1.getMap(mapName);
        final IMap<Integer, Integer> map2 = node2.getMap(mapName);

        int size = 1000;

        // fill map-1
        for (int i = 0; i < size; i++) {
            map1.put(i, i);
        }

        // fill Near Cache on node-1
        for (int i = 0; i < size; i++) {
            map1.get(i);
        }

        // fill Near Cache on node-2
        for (int i = 0; i < size; i++) {
            map2.get(i);
        }

        map1.evictAll();

        assertTrueEventually(() -> {
            NearCache nearCache1 = ((NearCachedMapProxyImpl) map1).getNearCache();
            NearCache nearCache2 = ((NearCachedMapProxyImpl) map2).getNearCache();
            assertEquals(0, nearCache1.size() + nearCache2.size());
        });
    }

    protected Config getConfig(String mapName) {
        MapConfig mapConfig = getMapConfig(mapName);

        return getBaseConfig()
                .addMapConfig(mapConfig);
    }

    protected MapConfig getMapConfig(String mapName) {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(mapName);

        return new MapConfig(mapName)
                .setNearCacheConfig(nearCacheConfig);
    }

    protected NearCacheConfig getNearCacheConfig(String mapName) {
        return new NearCacheConfig(mapName)
                .setInMemoryFormat(OBJECT)
                .setInvalidateOnChange(true)
                .setCacheLocalEntries(true);
    }

    private static void configureBatching(Config config, int batchSize, int period) {
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "true");
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), valueOf(batchSize));
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), valueOf(period));
    }
}
