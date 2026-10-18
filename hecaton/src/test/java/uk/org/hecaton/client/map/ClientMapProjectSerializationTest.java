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

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.AbstractMapQueryIterableTest.GetValueProjection;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapProjectSerializationTest extends HecatonTestSupport {
    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @Before
    public void setup() {

    }

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test
    public void testProjectObjectShouldDeserializedOnlyTwice() {
        // One deserialization on server when object is accessed from transform
        // Second deserialization on client side when result passed to user
        hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient();

        IMap<Integer, OnlyDeserializedTwiceObject> map = client.getMap("test");
        OnlyDeserializedTwiceObject value = new OnlyDeserializedTwiceObject("test");
        map.put(1, value);
        Collection<OnlyDeserializedTwiceObject> result = map.project(new GetValueProjection<>());

        assertEquals(Collections.singletonList(value), result);
    }

    private static class OnlyDeserializedTwiceObject implements DataSerializable {

        private String value;

        private static AtomicInteger readCalled = new AtomicInteger();

        OnlyDeserializedTwiceObject() {
        }

        OnlyDeserializedTwiceObject(String value) {
            this.value = value;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            if (readCalled.incrementAndGet() > 2) {
                throw new AssertionError("Read called more than twice!!!");
            }
            value = in.readString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof OnlyDeserializedTwiceObject)) {
                return false;
            }

            OnlyDeserializedTwiceObject that = (OnlyDeserializedTwiceObject) o;

            return value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }
    }
}
