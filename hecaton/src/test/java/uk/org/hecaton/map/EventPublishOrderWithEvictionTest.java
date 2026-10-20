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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EvictionPolicy;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.EntryAdapter;
import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.EntryEventType;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class EventPublishOrderWithEvictionTest extends HecatonTestSupport {

    @Test
    public void testEntryEvictEventsEmitted_afterAddEvents() {
        final int maxSize = 10;
        IMap<Integer, Integer> map = createMap(maxSize);
        EventOrderAwareEntryListener entryListener = new EventOrderAwareEntryListener();
        map.addEntryListener(entryListener, true);

        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < maxSize; i++) {
                map.put(i, i);
            }
        }

        sleepMillis(3456);

        assertEmittedEventsOrder(entryListener);
    }

    private void assertEmittedEventsOrder(EventOrderAwareEntryListener entryListener) {
        Map<Integer, List<EntryEventType>> eventsPerKey = new HashMap<>();
        List<EntryEvent> events = entryListener.getOrderedEvents();
        for (EntryEvent event : events) {
            Integer key = (Integer) event.getKey();
            List<EntryEventType> eventTypes = eventsPerKey.get(key);
            if (eventTypes == null) {
                eventTypes = new ArrayList<>();
                eventsPerKey.put(key, eventTypes);
            }

            EntryEventType eventType = event.getEventType();
            eventTypes.add(eventType);
        }

        Set<Map.Entry<Integer, List<EntryEventType>>> entries = eventsPerKey.entrySet();
        for (Map.Entry<Integer, List<EntryEventType>> entry : entries) {
            List<EntryEventType> eventTypes = entry.getValue();
            EntryEventType prev = null;
            for (int i = 0; i < eventTypes.size(); i++) {
                final EntryEventType eventType = eventTypes.get(i);
                if (i == 0) {
                    assertEquals(EntryEventType.ADDED, eventType);
                    prev = eventType;
                    continue;
                }

                if (prev == EntryEventType.ADDED) {
                    assertEquals(EntryEventType.EVICTED, eventType);
                } else if (prev == EntryEventType.EVICTED) {
                    assertEquals(EntryEventType.ADDED, eventType);
                }

                prev = eventType;
            }
        }
    }

    private <K, V> IMap<K, V> createMap(int maxSize) {
        String mapName = randomMapName();
        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.getEvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LRU).setSize(maxSize);

        return createHecatonInstance(config).getMap(mapName);
    }

    private static final class EventOrderAwareEntryListener extends EntryAdapter {

        private final List<EntryEvent> orderedEvents = new CopyOnWriteArrayList<>();

        @Override
        public void entryEvicted(EntryEvent event) {
            orderedEvents.add(event);
        }

        @Override
        public void entryAdded(EntryEvent event) {
            orderedEvents.add(event);
        }

        List<EntryEvent> getOrderedEvents() {
            return orderedEvents;
        }
    }
}
