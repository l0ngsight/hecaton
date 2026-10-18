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

import uk.org.hecaton.core.EntryAdapter;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.MapEvent;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapEvictAllTest extends AbstractClientMapTest {

    @Test
    public void evictAll_firesEvent() {
        final String mapName = randomMapName();
        final IMap<Object, Object> map = client.getMap(mapName);
        final CountDownLatch evictedEntryCount = new CountDownLatch(3);
        map.addEntryListener(new EntryAdapter<>() {
            @Override
            public void mapEvicted(MapEvent event) {
                final int affected = event.getNumberOfEntriesAffected();
                for (int i = 0; i < affected; i++) {
                    evictedEntryCount.countDown();
                }

            }
        }, true);

        map.put(1, 1);
        map.put(2, 1);
        map.put(3, 1);
        map.evictAll();

        assertOpenEventually(evictedEntryCount);
        assertEquals(0, map.size());
    }

    @Test
    public void evictAll_firesOnlyOneEvent() throws Exception {
        final String mapName = randomMapName();
        final IMap<Object, Object> map = client.getMap(mapName);
        final CountDownLatch eventCount = new CountDownLatch(2);
        map.addEntryListener(new EntryAdapter<>() {
            @Override
            public void mapEvicted(MapEvent event) {
                eventCount.countDown();

            }
        }, true);

        map.put(1, 1);
        map.put(2, 1);
        map.put(3, 1);
        map.evictAll();

        assertFalse(eventCount.await(10, TimeUnit.SECONDS));
        assertEquals(1, eventCount.getCount());
    }
}
