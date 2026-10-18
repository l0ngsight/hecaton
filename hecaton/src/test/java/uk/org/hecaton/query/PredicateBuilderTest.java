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

package uk.org.hecaton.query;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.query.PredicateBuilder.EntryObject;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class PredicateBuilderTest extends HecatonTestSupport {

    @Test
    public void get_keyAttribute() {
        HecatonInstance hz = createHecatonInstance();

        EntryObject entryObject = Predicates.newPredicateBuilder().getEntryObject();
        Predicate predicate = entryObject.key().get("id").equal("10").and(entryObject.get("name").equal("value1"));

        IMap<Id, Value> hecatonLookupMap = hz.getMap("someMap");

        hecatonLookupMap.put(new Id("10"), new Value("value1"));
        hecatonLookupMap.put(new Id("20"), new Value("value2"));
        hecatonLookupMap.put(new Id("30"), new Value("value3"));

        Collection<Value> result = hecatonLookupMap.values(predicate);
        assertEquals(1, result.size());
        assertContains(result, new Value("value1"));
    }

    @Test
    public void get_key() {
        HecatonInstance hz = createHecatonInstance();

        EntryObject entryObject = Predicates.newPredicateBuilder().getEntryObject();
        Predicate predicate = entryObject.key().equal(10L);

        IMap<Integer, Integer> hecatonLookupMap = hz.getMap("someMap");

        hecatonLookupMap.put(10, 1);
        hecatonLookupMap.put(30, 2);

        Collection<Integer> result = hecatonLookupMap.values(predicate);
        assertEquals(1, result.size());
        assertContains(result, 1);
    }

    @Test
    public void get_this() {
        HecatonInstance hz = createHecatonInstance();

        EntryObject entryObject = Predicates.newPredicateBuilder().getEntryObject();
        Predicate predicate = entryObject.get("this").equal(1L);

        IMap<Integer, Integer> hecatonLookupMap = hz.getMap("someMap");

        hecatonLookupMap.put(10, 1);
        hecatonLookupMap.put(30, 2);

        Collection<Integer> result = hecatonLookupMap.values(predicate);
        assertEquals(1, result.size());
        assertContains(result, 1);
    }

    @Test
    public void get_attribute() {
        HecatonInstance hz = createHecatonInstance();

        EntryObject entryObject = Predicates.newPredicateBuilder().getEntryObject();
        Predicate predicate = entryObject.get("id").equal("10");

        IMap<Integer, Id> hecatonLookupMap = hz.getMap("someMap");

        hecatonLookupMap.put(1, new Id("10"));
        hecatonLookupMap.put(2, new Id("20"));
        hecatonLookupMap.put(3, new Id("30"));

        Collection<Id> result = hecatonLookupMap.values(predicate);
        assertEquals(1, result.size());
        assertContains(result, new Id("10"));
    }

    private static final class Id implements Serializable {

        private String id;

        private Id(String id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Id id1 = (Id) o;
            if (id != null ? !id.equals(id1.id) : id1.id != null) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            return id != null ? id.hashCode() : 0;
        }
    }

    private static final class Value implements Serializable {

        private String name;

        private Value(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Value value = (Value) o;
            if (name != null ? !name.equals(value.name) : value.name != null) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }
    }
}
