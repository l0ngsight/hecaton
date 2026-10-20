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

package uk.org.hecaton.internal.serialization.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializableFactory;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.nio.serialization.Portable;
import uk.org.hecaton.nio.serialization.PortableFactory;
import uk.org.hecaton.nio.serialization.PortableReader;
import uk.org.hecaton.nio.serialization.PortableWriter;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class SerializersHecatonInstanceAwareTest extends HecatonTestSupport {

    @Test
    public void testPortableFactoryInstance() {
        HecatonInstanceAwarePortableFactory factory = new HecatonInstanceAwarePortableFactory();

        Config config = new Config();
        config.getSerializationConfig().addPortableFactory(1, factory);

        HecatonInstance instance = createHecatonInstance(config);
        Map<String, PortablePerson> map = instance.getMap("map");

        map.put("1", new PortablePerson());
        PortablePerson person = map.get("1");
        assertNotNull("HecatonInstance should have been set", person.hz);
    }

    @Test
    public void testPortableFactoryClass() {
        Config config = new Config();
        config.getSerializationConfig()
                .addPortableFactoryClass(1, HecatonInstanceAwarePortableFactory.class.getName());

        HecatonInstance instance = createHecatonInstance(config);
        Map<String, PortablePerson> map = instance.getMap("map");
        map.put("1", new PortablePerson());
        PortablePerson person = map.get("1");
        assertNotNull("HecatonInstance should have been set", person.hz);
    }

    @Test
    public void testDataSerializableFactoryInstance() {
        HecatonInstanceAwareDataSerializableFactory factory = new HecatonInstanceAwareDataSerializableFactory();

        Config config = new Config();
        config.getSerializationConfig().addDataSerializableFactory(1, factory);

        HecatonInstance instance = createHecatonInstance(config);
        Map<String, DataSerializablePerson> map = instance.getMap("map");
        map.put("1", new DataSerializablePerson());
        DataSerializablePerson person = map.get("1");
        assertNotNull("HecatonInstance should have been set", person.hz);
    }

    @Test
    public void testDataSerializableFactoryClass() {
        Config config = new Config();
        config.getSerializationConfig().addDataSerializableFactoryClass(1,
                HecatonInstanceAwareDataSerializableFactory.class.getName());

        HecatonInstance instance = createHecatonInstance(config);
        Map<String, DataSerializablePerson> map = instance.getMap("map");
        map.put("1", new DataSerializablePerson());
        DataSerializablePerson person = map.get("1");
        assertNotNull("HecatonInstance should have been set", person.hz);
    }

    private static class HecatonInstanceAwarePortableFactory implements PortableFactory, HecatonInstanceAware {

        private HecatonInstance hz;

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            this.hz = hecatonInstance;
        }

        @Override
        public Portable create(int classId) {
            PortablePerson p = new PortablePerson();
            p.hz = hz;
            return p;
        }
    }

    private static class PortablePerson implements Portable {
        private HecatonInstance hz;

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
        }
    }

    private static class HecatonInstanceAwareDataSerializableFactory
            implements DataSerializableFactory, HecatonInstanceAware {

        private HecatonInstance hz;

        @Override
        public void setHecatonInstance(HecatonInstance hecatonInstance) {
            this.hz = hecatonInstance;
        }

        @Override
        public IdentifiedDataSerializable create(int typeId) {
            DataSerializablePerson p = new DataSerializablePerson();
            p.hz = hz;
            return p;
        }
    }

    private static class DataSerializablePerson implements IdentifiedDataSerializable {

        private HecatonInstance hz;

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }
}
