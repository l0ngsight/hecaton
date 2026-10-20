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

package uk.org.hecaton.client.map.impl.nearcache;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.EvictionPolicy;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapNearCacheEvictionTest extends HecatonTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void near_cache_size_equals_map_size_when_eviction_policy_is_none() {
        String mapName = "test";
        NearCacheConfig nearCacheConfig = new NearCacheConfig(mapName);
        nearCacheConfig.getEvictionConfig().setEvictionPolicy(EvictionPolicy.NONE);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addNearCacheConfig(nearCacheConfig);

        HecatonInstance server = factory.newHecatonInstance();
        HecatonInstance client = factory.newHecatonClient(clientConfig);
        IMap<Integer, Integer> map = client.getMap(mapName);

        // populate map
        int mapSize = 99_999;
        for (int i = 0; i < mapSize; i++) {
            map.set(i, i);
        }

        // populate near-cache
        for (int i = 0; i < mapSize; i++) {
            map.get(i);
        }

        assertEquals(mapSize, map.getLocalMapStats()
                .getNearCacheStats().getOwnedEntryCount());
    }

    @Test
    public void no_more_entries_than_max_near_cache_size_when_eviction_policy_is_not_none() {
        String mapName = "mapName";
        NearCacheConfig nearCacheConfig = new NearCacheConfig(mapName);
        int maxNearCacheSize = 9_999;
        nearCacheConfig.getEvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setSize(maxNearCacheSize);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addNearCacheConfig(nearCacheConfig);

        HecatonInstance server = factory.newHecatonInstance();
        HecatonInstance client = factory.newHecatonClient(clientConfig);
        IMap<Integer, Integer> map = client.getMap(mapName);

        // populate map
        int mapSize = 99_999;
        for (int i = 0; i < mapSize; i++) {
            map.set(i, i);
        }

        // populate near-cache
        for (int i = 0; i < mapSize; i++) {
            map.get(i);
        }

        assertEquals(maxNearCacheSize, map.getLocalMapStats()
                .getNearCacheStats().getOwnedEntryCount());
    }
}
