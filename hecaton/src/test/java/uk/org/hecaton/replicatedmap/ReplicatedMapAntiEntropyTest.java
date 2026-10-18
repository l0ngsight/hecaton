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

package uk.org.hecaton.replicatedmap;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.SerializationConfig;
import uk.org.hecaton.config.SerializerConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.StreamSerializer;
import uk.org.hecaton.replicatedmap.impl.operation.PutOperation;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapAntiEntropyTest extends ReplicatedMapAbstractTest {

    @After
    public void cleanup() {
        System.clearProperty("hecaton.serialization.custom.override");
    }

    @Test
    public void testMapConvergesToSameValueWhenMissingReplicationUpdate() {
        String mapName = randomMapName();
        System.setProperty("hecaton.serialization.custom.override", "true");

        SerializerConfig serializerConfig = new SerializerConfig()
                .setTypeClassName(PutOperation.class.getName())
                .setImplementation(new PutOperationWithNoReplicationSerializer());

        SerializationConfig serializationConfig = new SerializationConfig()
                .addSerializerConfig(serializerConfig);

        Config config = new Config()
                .setSerializationConfig(serializationConfig);

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance instance1 = factory.newHecatonInstance(config);
        HecatonInstance instance2 = factory.newHecatonInstance(config);
        HecatonInstance instance3 = factory.newHecatonInstance(config);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap(mapName);
        final ReplicatedMap<Object, Object> map2 = instance2.getReplicatedMap(mapName);
        final ReplicatedMap<Object, Object> map3 = instance3.getReplicatedMap(mapName);
        final String key = generateKeyOwnedBy(instance2);
        final String value = randomString();
        map1.put(key, value);
        assertEquals(value, map1.get(key));
        assertTrueEventually(() -> {
            assertEquals(value, map2.get(key));
            assertEquals(value, map3.get(key));
        });
    }

    public class PutOperationWithNoReplicationSerializer implements StreamSerializer<PutOperation> {

        @Override
        public void write(ObjectDataOutput out, PutOperation object) throws IOException {
            object.writeData(out);
        }

        @Override
        public PutOperation read(ObjectDataInput in) throws IOException {
            final PutOperationWithNoReplication operation = new PutOperationWithNoReplication();
            operation.readData(in);
            return operation;
        }

        @Override
        public int getTypeId() {
            return 8778;
        }

        @Override
        public void destroy() {
        }
    }

    class PutOperationWithNoReplication extends PutOperation {

        PutOperationWithNoReplication() {
        }

        @Override
        protected Collection<Address> getMemberAddresses() {
            return Collections.emptyList();
        }
    }
}
