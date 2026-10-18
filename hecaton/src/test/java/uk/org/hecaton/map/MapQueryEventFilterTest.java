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
import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.impl.MapListenerAdapter;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import static uk.org.hecaton.map.impl.event.MapEventPublisherImpl.PROP_LISTENER_WITH_PREDICATE_PRODUCES_NATURAL_EVENT_TYPES;
import static uk.org.hecaton.query.Predicates.greaterEqual;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapQueryEventFilterTest extends HecatonTestSupport {

    @Test
    public void test_add_and_update_event_counts_when_natural_event_types_enabled() {
        // expect 1 add event when age = 3 because of natural filtering
        // expect 1 update event when age >= 3
        test_expected_event_types(true, 1, 1);
    }

    /**
     * This tests default behaviour.
     */
    @Test
    public void test_add_and_update_event_counts_when_natural_event_types_disabled() {
        // expect 0 add event when age >= 3
        // expect 2 update events when age >= 3 because of disabled natural filtering
        test_expected_event_types(false, 0, 2);
    }

    private void test_expected_event_types(boolean withNaturalEventType, final int numOfAddEventExpected,
                                           final int numOfUpdateEventExpected) {
        Config config = getConfig();
        config.setProperty(PROP_LISTENER_WITH_PREDICATE_PRODUCES_NATURAL_EVENT_TYPES,
                String.valueOf(withNaturalEventType));
        HecatonInstance node = createHecatonInstance(config);

        final AtomicInteger addedEntryCount = new AtomicInteger();
        final AtomicInteger updatedEntryCount = new AtomicInteger();

        IMap<Integer, Employee> map = node.getMap("test");

        map.addEntryListener(new MapListenerAdapter<Integer, Employee>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Employee> event) {
                addedEntryCount.incrementAndGet();
            }

            @Override
            public void entryUpdated(EntryEvent<Integer, Employee> event) {
                updatedEntryCount.incrementAndGet();
            }
        }, greaterEqual("age", 3), true);

        // update same key, multiple times.
        int key = 1;
        map.set(key, new Employee(1));
        map.set(key, new Employee(2));
        map.set(key, new Employee(3));
        map.set(key, new Employee(4));

        assertTrueEventually(() -> {
            assertEquals(numOfAddEventExpected, addedEntryCount.get());
            assertEquals(numOfUpdateEventExpected, updatedEntryCount.get());
        });
    }

    static final class Employee implements Serializable {
        int age;

        Employee(int age) {
            this.age = age;
        }
    }
}
