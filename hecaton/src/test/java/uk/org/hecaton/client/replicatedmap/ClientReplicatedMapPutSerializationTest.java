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

package uk.org.hecaton.client.replicatedmap;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.replicatedmap.ReplicatedMap;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientReplicatedMapPutSerializationTest extends HecatonTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    private static AtomicInteger deSerializationCount = new AtomicInteger();

    @Test
    public void testPutShouldNotDeserializeData() {
        String mapName = randomName();
        Config config = new Config();
        config.getReplicatedMapConfig(mapName).setInMemoryFormat(InMemoryFormat.BINARY);
        factory.newHecatonInstance(config);
        HecatonInstance client = factory.newHecatonClient();
        ReplicatedMap<String, SerializationCountingData> map = client.getReplicatedMap(mapName);
        String key = randomString();
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
