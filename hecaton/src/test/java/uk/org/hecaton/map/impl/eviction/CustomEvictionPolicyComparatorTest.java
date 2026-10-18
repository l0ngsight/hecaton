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
import uk.org.hecaton.core.EntryView;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.MapEvictionPolicyComparator;
import uk.org.hecaton.map.listener.EntryEvictedListener;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.config.MaxSizePolicy.PER_PARTITION;
import static uk.org.hecaton.map.impl.eviction.Evictor.SAMPLE_COUNT;
import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_COUNT;
import static java.lang.String.format;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CustomEvictionPolicyComparatorTest extends HecatonTestSupport {

    @Test
    public void custom_eviction_policy_removes_correct_entries() {
        int sampleCount = SAMPLE_COUNT;

        Config config = getConfig();
        config.setProperty(PARTITION_COUNT.getName(), "1");
        String mapName = "default";

        config.getMapConfig(mapName)
                .getEvictionConfig()
                .setComparator(new OddEvictor())
                .setMaxSizePolicy(PER_PARTITION)
                .setSize(sampleCount);

        HecatonInstance instance = createHecatonInstance(config);
        IMap<Integer, Integer> map = instance.getMap(mapName);

        final CountDownLatch eventLatch = new CountDownLatch(1);
        final Queue<Integer> evictedKeys = new ConcurrentLinkedQueue<>();
        map.addEntryListener((EntryEvictedListener<Integer, Integer>) event -> {
            evictedKeys.add(event.getKey());
            eventLatch.countDown();
        }, false);

        for (int i = 0; i < sampleCount + 1; i++) {
            map.put(i, i);
        }

        assertOpenEventually("No eviction occurred", eventLatch);

        for (Integer key : evictedKeys) {
            String message = format("Evicted key should be an odd number, but found %d", key);
            assertNotEquals(message, 0, key % 2);
        }
    }

    private static class OddEvictor
            implements MapEvictionPolicyComparator<Integer, Integer> {

        @Override
        public int compare(EntryView e1, EntryView e2) {
            assertNotNull(e1);
            assertNotNull(e2);

            assertNotEquals(e1, e2);

            assertNotEquals("Hash code of e1 should not be zero", 0, e1.hashCode());
            assertNotEquals("Hash code of e2 should not be zero", 0, e2.hashCode());

            assertNotNull(e1.toString());
            assertNotNull(e2.toString());

            Integer key1 = (Integer) e1.getKey();
            if (key1 % 2 != 0) {
                return -1;
            }

            Integer key2 = (Integer) e2.getKey();
            if (key2 % 2 != 0) {
                return 1;
            }

            return 0;
        }
    }
}
