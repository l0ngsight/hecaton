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

package uk.org.hecaton.map.impl.eviction;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.util.CollectionUtil;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExpiryAndLockTest extends HecatonTestSupport {

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Test
    public void locking_does_not_cause_expired_keys_live_forever() {
        final String KEY = "key";

        final HecatonInstance node = createHecatonInstance(getConfig());
        try {
            IMap<String, String> map = node.getMap("test");
            // after 1 second entry should be evicted
            map.set(KEY, "value", 4, TimeUnit.SECONDS);
            // short time after adding it to the map, all ok
            map.lock(KEY);
            Object object = map.get(KEY);
            map.unlock(KEY);
            assertNotNull(object);

            sleepAtLeastSeconds(5);

            // more than one second after adding it, now it should be away
            map.lock(KEY);
            object = map.get(KEY);
            map.unlock(KEY);
            assertNull(object);
        } finally {
            node.shutdown();
        }
    }

    @Test
    public void locked_keys_are_reachable_over_index_after_they_expired() {
        Config config = getConfig();
        config.getMapConfig("default")
                .setTimeToLiveSeconds(2)
                .addIndexConfig(new IndexConfig(IndexType.SORTED, "this"));
        HecatonInstance node = createHecatonInstance(config);
        IMap<Integer, Integer> indexedMap = node.getMap("indexed");

        int keyCount = 1_000;
        for (int i = 0; i < keyCount; i++) {
            indexedMap.set(i, i);
            indexedMap.lock(i);
        }

        assertTrueAllTheTime(() -> {
            Set<Map.Entry<Integer, Integer>> entries = indexedMap.entrySet(Predicates.sql("this >= 0"));
            int entrySetSize = CollectionUtil.isEmpty(entries) ? 0 : entries.size();
            assertEquals(keyCount, entrySetSize);
        }, 5);
    }

    @Test
    public void key_size_map_size_zero_after_eviction() {
        Config config = new Config();

        String mapName = "discrepancy-map";
        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setMaxIdleSeconds(10);

        // Add an incorrect index that uses a non-existing field
        IndexConfig indexConfig = new IndexConfig(IndexConfig.DEFAULT_TYPE, "d");
        mapConfig.addIndexConfig(indexConfig);
        config.addMapConfig(mapConfig);

        HecatonInstance hecatonInstance = createHecatonInstance(config);
        try {

            IMap<Integer, Session> sessionMap = hecatonInstance.getMap(mapName);
            Session session = new Session("a", "b");
            sessionMap.putAsync(1, session);

            // Even though the index is incorrect, after eviction map and keySet sizes must be 0
            assertTrueEventually(() -> {
                assertThat(sessionMap.size()).isZero();
                assertThat(sessionMap.keySet().size()).isZero();
            });

        } finally {
            hecatonInstance.shutdown();
        }
    }

    private record Session(String a, String b) implements Serializable {
    }
}
