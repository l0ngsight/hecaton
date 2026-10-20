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
import uk.org.hecaton.core.EntryAdapter;
import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.EntryListener;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryListenerTest extends HecatonTestSupport {

    @Test
    public void testMapQueryListener() throws InterruptedException {
        Config config = getConfig();
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory(3);
        HecatonInstance instance1 = nodeFactory.newHecatonInstance(config);
        HecatonInstance instance2 = nodeFactory.newHecatonInstance(config);
        HecatonInstance instance3 = nodeFactory.newHecatonInstance(config);

        final IMap<Object, Object> map = instance1.getMap("testMapQueryListener");
        final Object[] addedKey = new Object[1];
        final Object[] addedValue = new Object[1];
        final Object[] updatedKey = new Object[1];
        final Object[] oldValue = new Object[1];
        final Object[] newValue = new Object[1];
        final Object[] removedKey = new Object[1];
        final Object[] removedValue = new Object[1];

        EntryListener<Object, Object> listener = new EntryAdapter<>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                addedKey[0] = event.getKey();
                addedValue[0] = event.getValue();
            }

            @Override
            public void entryRemoved(EntryEvent<Object, Object> event) {
                removedKey[0] = event.getKey();
                removedValue[0] = event.getOldValue();
            }

            @Override
            public void entryUpdated(EntryEvent<Object, Object> event) {
                updatedKey[0] = event.getKey();
                oldValue[0] = event.getOldValue();
                newValue[0] = event.getValue();
            }

            @Override
            public void entryEvicted(EntryEvent<Object, Object> event) {
            }

            @Override
            public void mapEvicted(MapEvent event) {
            }

            @Override
            public void mapCleared(MapEvent event) {
            }
        };

        map.addEntryListener(listener, new StartsWithPredicate("a"), null, true);
        map.put("key1", "abc");
        map.put("key2", "bcd");
        map.put("key2", "axyz");
        map.remove("key1");
        Thread.sleep(1000);

        assertEquals("key1", addedKey[0]);
        assertEquals("abc", addedValue[0]);
        assertEquals("key2", updatedKey[0]);
        assertEquals("bcd", oldValue[0]);
        assertEquals("axyz", newValue[0]);
        assertEquals("key1", removedKey[0]);
        assertEquals("abc", removedValue[0]);
    }

    @Test
    public void testMapQueryListener2() throws InterruptedException {
        Config cfg = getConfig();
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory(3);
        HecatonInstance instance1 = nodeFactory.newHecatonInstance(cfg);
        HecatonInstance instance2 = nodeFactory.newHecatonInstance(cfg);
        HecatonInstance instance3 = nodeFactory.newHecatonInstance(cfg);

        final IMap<Object, Object> map = instance1.getMap("testMapQueryListener2");
        final AtomicInteger addCount = new AtomicInteger();


        EntryListener<Object, Object> listener = new EntryAdapter<>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                addCount.incrementAndGet();
            }
        };

        Predicate<Object, Object> predicate = Predicates.sql("age >= 50");
        map.addEntryListener(listener, predicate, null, false);
        int size = 100;
        for (int i = 0; i < size; i++) {
            Person person = new Person("name", i);
            map.put(i, person);
        }
        Thread.sleep(1000);
        assertEquals(50, addCount.get());
    }

    static class StartsWithPredicate implements Predicate<Object, Object>, Serializable {
        String pref;

        StartsWithPredicate(String pref) {
            this.pref = pref;
        }

        @Override
        public boolean apply(Map.Entry<Object, Object> mapEntry) {
            String val = (String) mapEntry.getValue();
            if (val == null) {
                return false;
            }
            if (val.startsWith(pref)) {
                return true;
            }
            return false;
        }
    }

    static class Person implements Serializable {

        String name;
        int age;

        Person() {
        }

        Person(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }
}
