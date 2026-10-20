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

package uk.org.hecaton.internal.util.comparators;

import uk.org.hecaton.partition.PartitioningStrategy;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.internal.serialization.impl.HeapData;
import uk.org.hecaton.map.impl.record.Person;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.partition.strategy.DefaultPartitioningStrategy;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.test.HecatonTestSupport;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("WeakerAccess")
public abstract class AbstractValueComparatorTest extends HecatonTestSupport {

    SerializationService serializationService;
    PartitioningStrategy partitioningStrategy;

    ValueComparator comparator;

    Person object1;
    Person object2;

    Data data1;
    Data data2;
    Data nullData;

    @Before
    public final void init() {
        serializationService = createSerializationService();
        partitioningStrategy = new DefaultPartitioningStrategy();

        object1 = new Person("Alice");
        object2 = new Person("Bob");

        data1 = serializationService.toData(object1);
        data2 = serializationService.toData(object2);
        nullData = new HeapData(new byte[0]);
    }

    @Test
    public void testIsEqual() {
        newRecordComparator();

        assertTrue(comparator.isEqual(null, null, serializationService));
        assertTrue(comparator.isEqual(object1, object1, serializationService));
        assertTrue(comparator.isEqual(object1, data1, serializationService));
        assertTrue(comparator.isEqual(data1, data1, serializationService));
        assertTrue(comparator.isEqual(data1, object1, serializationService));
        assertTrue(comparator.isEqual(nullData, nullData, serializationService));

        assertFalse(comparator.isEqual(null, object1, serializationService));
        assertFalse(comparator.isEqual(null, data1, serializationService));
        assertFalse(comparator.isEqual(null, nullData, serializationService));
        assertFalse(comparator.isEqual(object1, null, serializationService));
        assertFalse(comparator.isEqual(object1, nullData, serializationService));
        assertFalse(comparator.isEqual(object1, object2, serializationService));
        assertFalse(comparator.isEqual(object1, data2, serializationService));
        assertFalse(comparator.isEqual(data1, null, serializationService));
        assertFalse(comparator.isEqual(data1, nullData, serializationService));
        assertFalse(comparator.isEqual(data1, object2, serializationService));
        assertFalse(comparator.isEqual(data1, data2, serializationService));
        assertFalse(comparator.isEqual(nullData, null, serializationService));
        assertFalse(comparator.isEqual(nullData, object1, serializationService));
        assertFalse(comparator.isEqual(nullData, data1, serializationService));
    }

    @Test
    public void testIsEqual_withCustomPartitioningStrategy() {
        partitioningStrategy = new PersonPartitioningStrategy();

        data1 = serializationService.toData(object1, partitioningStrategy);
        data2 = serializationService.toData(object2, partitioningStrategy);

        testIsEqual();
    }

    abstract void newRecordComparator();

    InternalSerializationService createSerializationService() {
        return new DefaultSerializationServiceBuilder().build();
    }

    static class PersonPartitioningStrategy implements PartitioningStrategy<Person> {

        @Override
        public Object getPartitionKey(Person key) {
            return key.getName();
        }
    }
}
