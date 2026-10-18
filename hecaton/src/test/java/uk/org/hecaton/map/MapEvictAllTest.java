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

package uk.org.hecaton.map;

import uk.org.hecaton.core.EntryAdapter;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapEvictAllTest extends HecatonTestSupport {

    @Test
    public void testEvictAll_firesEvent() {
        final CountDownLatch countDownLatch = new CountDownLatch(1000);
        HecatonInstance instance = createHecatonInstance(getConfig());
        IMap<Integer, Integer> map = instance.getMap(randomMapName());
        map.addLocalEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void mapEvicted(MapEvent event) {
                int numberOfEntries = event.getNumberOfEntriesAffected();
                for (int i = 0; i < numberOfEntries; i++) {
                    countDownLatch.countDown();
                }
            }
        });

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }
        map.evictAll();

        assertOpenEventually(countDownLatch);
    }

    @Test
    public void testEvictAll_preserveLockedKeys() {
        int numberOfEntries = 1000;
        int numberOfLockedKeys = 123;
        int expectedNumberOfEvictedKeys = numberOfEntries - numberOfLockedKeys;
        final CountDownLatch countDownLatch = new CountDownLatch(expectedNumberOfEvictedKeys);
        HecatonInstance node = createHecatonInstance(getConfig());
        IMap<Integer, Integer> map = node.getMap(randomMapName());
        map.addLocalEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void mapEvicted(MapEvent event) {
                int numberOfEntries = event.getNumberOfEntriesAffected();
                for (int i = 0; i < numberOfEntries; i++) {
                    countDownLatch.countDown();
                }
            }
        });

        for (int i = 0; i < numberOfEntries; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < numberOfLockedKeys; i++) {
            map.lock(i);
        }
        map.evictAll();

        assertOpenEventually(countDownLatch);
        assertEquals(0, countDownLatch.getCount());
        assertEquals(numberOfLockedKeys, map.size());
    }

    @Test
    public void testEvictAll_onBackup() {
        int numberOfEntries = 10000;
        String mapName = randomMapName();
        final CountDownLatch countDownLatch = new CountDownLatch(numberOfEntries);
        TestHecatonInstanceFactory instanceFactory = createHecatonInstanceFactory(2);
        HecatonInstance node1 = instanceFactory.newHecatonInstance(getConfig());
        HecatonInstance node2 = instanceFactory.newHecatonInstance(getConfig());
        final IMap<Integer, Integer> map1 = node1.getMap(mapName);
        final IMap<Integer, Integer> map2 = node2.getMap(mapName);
        map1.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void mapEvicted(MapEvent event) {
                int numberOfEntries = event.getNumberOfEntriesAffected();
                for (int i = 0; i < numberOfEntries; i++) {
                    countDownLatch.countDown();
                }
            }
        }, false);

        for (int i = 0; i < numberOfEntries; i++) {
            map1.put(i, i);
        }
        map1.evictAll();

        assertOpenEventually(countDownLatch);
        assertEquals(0, countDownLatch.getCount());
        assertTrueEventually(() -> assertEquals(0, map1.getLocalMapStats().getHeapCost()));
        assertTrueEventually(() -> assertEquals(0, map2.getLocalMapStats().getHeapCost()));
    }
}
