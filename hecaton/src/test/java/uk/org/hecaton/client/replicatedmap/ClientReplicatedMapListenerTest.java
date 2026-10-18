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
import uk.org.hecaton.core.EntryAdapter;
import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.replicatedmap.AbstractReplicatedMapListenerTest;
import uk.org.hecaton.replicatedmap.ReplicatedMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HecatonParallelClassRunner.class)
public class ClientReplicatedMapListenerTest extends AbstractReplicatedMapListenerTest {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void no_key_value_deserialization_on_server_when_in_memory_format_is_binary() {
        final CountDownLatch eventReceivedLatch = new CountDownLatch(1);

        Config config = new Config();
        config.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);

        HecatonInstance server = factory.newHecatonInstance(config);
        HecatonInstance client = factory.newHecatonClient();

        ReplicatedMap<DeserializationCounter, DeserializationCounter> replicatedMap = client.getReplicatedMap("test");
        replicatedMap.addEntryListener(new EntryAdapter<>() {
            @Override
            public void onEntryEvent(EntryEvent<DeserializationCounter, DeserializationCounter> event) {
                eventReceivedLatch.countDown();
            }
        }, Predicates.alwaysTrue());

        DeserializationCounter key = new DeserializationCounter();
        DeserializationCounter value = new DeserializationCounter();

        replicatedMap.put(key, value);

        // wait to get event on client side
        assertOpenEventually(eventReceivedLatch);

        assertEquals(0, key.DESERIALIZATION_COUNT.get());
        assertEquals(0, value.DESERIALIZATION_COUNT.get());
    }

    public static class DeserializationCounter implements DataSerializable {

        protected static final AtomicInteger DESERIALIZATION_COUNT = new AtomicInteger();

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {

        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            DESERIALIZATION_COUNT.incrementAndGet();
        }
    }

    protected <K, V> ReplicatedMap<K, V> createClusterAndGetRandomReplicatedMap() {
        factory.newHecatonInstance();
        HecatonInstance client = factory.newHecatonClient();
        String mapName = randomMapName();
        return client.getReplicatedMap(mapName);
    }
}
