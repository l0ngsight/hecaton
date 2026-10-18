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
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapPutSerializationTest extends HecatonTestSupport {

    static AtomicInteger deSerializationCount = new AtomicInteger();

    @Test
    public void testPutShouldNotDeserializeData() {
        String mapName = randomName();
        Config config = new Config();
        config.getReplicatedMapConfig(mapName).setInMemoryFormat(InMemoryFormat.BINARY);
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory();
        HecatonInstance instance1 = nodeFactory.newHecatonInstance(config);
        HecatonInstance instance2 = nodeFactory.newHecatonInstance(config);
        ReplicatedMap<String, SerializationCountingData> map = instance1.getReplicatedMap(mapName);
        String key = generateKeyOwnedBy(instance2);
        SerializationCountingData value = new SerializationCountingData();
        map.put(key, value);
        map.put(key, value);

        // only deserialized once in the proxy
        assertEquals(1, deSerializationCount.get());
    }

    static class SerializationCountingData implements DataSerializable {

        SerializationCountingData() {
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            deSerializationCount.incrementAndGet();
        }
    }
}
