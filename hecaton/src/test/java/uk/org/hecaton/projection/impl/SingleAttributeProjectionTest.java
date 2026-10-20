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

package uk.org.hecaton.projection.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.projection.Projection;
import uk.org.hecaton.projection.Projections;
import uk.org.hecaton.query.QueryException;
import uk.org.hecaton.query.ReflectiveAttributeTestObject;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_COUNT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SingleAttributeProjectionTest extends HecatonTestSupport {

    @Test(expected = IllegalArgumentException.class)
    public void singleAttribute_attributeNull() {
        Projections.<Map.Entry<String, Person>, Double>singleAttribute(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void singleAttribute_attributeEmpty() {
        Projections.<Map.Entry<String, Person>, Double>singleAttribute("");
    }

    @Test
    public void singleAttribute() {
        IMap<String, Person> map = getMapWithNodeCount();
        populateMapWithPersons(map);

        Collection<Double> result = map.project(Projections.singleAttribute("age"));

        assertThat(result).containsExactlyInAnyOrder(1.0d, 4.0d, 7.0d);
    }

    @Test
    public void singleAttribute_key() {
        IMap<String, Person> map = getMapWithNodeCount();
        populateMapWithPersons(map);

        Collection<String> result = map.project(Projections.singleAttribute("__key"));

        assertThat(result).containsExactlyInAnyOrder("key1", "key2", "key3");
    }

    @Test
    public void singleAttribute_this() {
        IMap<String, Integer> map = getMapWithNodeCount();
        map.put("key1", 1);
        map.put("key2", 2);

        Collection<Integer> result = map.project(Projections.singleAttribute("this"));

        assertThat(result).containsExactlyInAnyOrder(1, 2);
    }

    @Test
    public void singleAttribute_emptyMap() {
        IMap<String, Person> map = getMapWithNodeCount();

        Collection<Double> result = map.project(Projections.singleAttribute("age"));

        assertEquals(0, result.size());
    }

    @Test
    public void singleAttribute_null() {
        IMap<String, Person> map = getMapWithNodeCount();
        map.put("key1", new Person(1.0d));
        map.put("007", new Person(null));

        Collection<Double> result = map.project(Projections.singleAttribute("age"));

        assertThat(result).containsExactlyInAnyOrder(null, 1.0d);
    }

    @Test
    public void singleAttribute_optional() {
        IMap<String, Person> map = getMapWithNodeCount();
        map.put("key1", new Person(1.0d));
        map.put("007", new Person(null));

        Collection<Double> result = map.project(Projections.singleAttribute("optionalAge"));

        assertThat(result).containsExactlyInAnyOrder(null, 1.0d);
    }

    @Test
    public void singleAttribute_nonExistingProperty() {
        IMap<String, Person> map = getMapWithNodeCount();
        populateMapWithPersons(map);

        Projection<Map.Entry<String, Person>, Double> projection = Projections.singleAttribute("age123");

        assertThatThrownBy(() -> map.project(projection))
                .isInstanceOf(QueryException.class);
    }

    @Test
    public void singleAttribute_restrictedLookup_staticField() {
        IMap<String, ReflectiveAttributeTestObject> map = getMapWithNodeCount();
        map.put("key1", new ReflectiveAttributeTestObject("a"));

        assertThatThrownBy(() -> map.project(Projections.singleAttribute("staticInt")))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("cannot be used for attribute extraction");
    }

    @Test
    public void singleAttribute_restrictedLookup_staticMethod() {
        IMap<String, ReflectiveAttributeTestObject> map = getMapWithNodeCount();
        map.put("key1", new ReflectiveAttributeTestObject("b"));

        assertThatThrownBy(() -> map.project(Projections.singleAttribute("getStaticValue")))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("cannot be used for attribute extraction");
    }

    @Test
    public void singleAttribute_restrictedLookup_voidMethod() {
        IMap<String, ReflectiveAttributeTestObject> map = getMapWithNodeCount();
        map.put("key1", new ReflectiveAttributeTestObject("c"));

        assertThatThrownBy(() -> map.project(Projections.singleAttribute("doVoid")))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("cannot be used for attribute extraction");
    }

    @Test
    public void singleAttribute_restrictedLookup_blockedClass() {
        IMap<String, ReflectiveAttributeTestObject> map = getMapWithNodeCount();
        map.put("key1", new ReflectiveAttributeTestObject("d"));

        assertThatThrownBy(() -> map.project(Projections.singleAttribute("hecatonInstance.name")))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("cannot be used for attribute extraction");
    }

    @Test
    public void singleAttribute_restrictedLookup_whenMatchingStaticAndGetter_thenGetterIsUsed() {
        IMap<String, ReflectiveAttributeTestObject> map = getMapWithNodeCount();
        map.put("key1", new ReflectiveAttributeTestObject("e"));

        Collection<String> result = map.project(Projections.singleAttribute("name"));
        assertThat(result).containsExactly("e");
    }

    private <K, V> IMap<K, V> getMapWithNodeCount() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(1);

        MapConfig mapConfig = new MapConfig()
                .setName("aggr")
                .setInMemoryFormat(InMemoryFormat.OBJECT);

        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "3")
                .addMapConfig(mapConfig);

        HecatonInstance instance = factory.newInstances(config)[0];
        return instance.getMap("aggr");
    }

    private void populateMapWithPersons(IMap<String, Person> map) {
        map.put("key1", new Person(1.0d));
        map.put("key2", new Person(4.0d));
        map.put("key3", new Person(7.0d));
    }

    public static class Person implements DataSerializable {

        public Double age;

        public Person() {
        }

        public Person(Double age) {
            this.age = age;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(age);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            age = in.readObject();
        }

        @SuppressWarnings("unused")
        public Optional<Double> optionalAge() {
            return Optional.ofNullable(age);
        }

    }

}
