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
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.monitor.impl.MemberPartitionStateImpl;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.partition.PartitionService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InMemoryFormatTest extends HecatonTestSupport {

    /**
     * if statistics enabled InMemoryFormat.Object does not work
     */
    @Test
    public void testIssue2622() {
        final String mapName = randomString();
        Config config = new Config();
        final MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        mapConfig.setStatisticsEnabled(true);
        config.addMapConfig(mapConfig);
        final HecatonInstance instance = createHecatonInstance(config);
        final IMap<String, SerializationValue> map = instance.getMap(mapName);
        final SerializationValue serializationValue = new SerializationValue();
        map.put("key", serializationValue);

        // EntryProcessor should not trigger de-serialization
        map.executeOnKey("key", entry -> null);
        assertEquals(1, SerializationValue.deSerializeCount.get());
    }

    @Test
    public void equals() {
        Config config = new Config();
        config.addMapConfig(new MapConfig("objectMap").setInMemoryFormat(InMemoryFormat.OBJECT));
        config.addMapConfig(new MapConfig("binaryMap").setInMemoryFormat(InMemoryFormat.BINARY));

        HecatonInstance hz = createHecatonInstance(config);

        Pair v1 = new Pair("a", "1");
        Pair v2 = new Pair("a", "2");

        IMap<String, Pair> objectMap = hz.getMap("objectMap");
        IMap<String, Pair> binaryMap = hz.getMap("binaryMap");

        objectMap.put("1", v1);
        binaryMap.put("1", v1);

        assertTrue(objectMap.containsValue(v1));
        assertTrue(objectMap.containsValue(v2));

        assertTrue(binaryMap.containsValue(v1));
        assertFalse(binaryMap.containsValue(v2));
    }

    @Test
    public void equalsReadLocalBackup() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);

        Config config = new Config();
        config.addMapConfig(new MapConfig("objectMap").setInMemoryFormat(InMemoryFormat.OBJECT).setReadBackupData(true));

        HecatonInstance hz1 = factory.newHecatonInstance(config);
        HecatonInstance hz2 = factory.newHecatonInstance(config);

        Pair pair = new Pair("a", "1");

        IMap<String, Pair> objectMap1 = hz1.getMap("objectMap");
        IMap<String, Pair> objectMap2 = hz2.getMap("objectMap");

        objectMap1.put("1", pair);
        Pair v1 = objectMap1.get("1");
        Pair v2 = objectMap1.get("1");

        Pair rv1 = objectMap2.get("1");
        Pair rv2 = objectMap2.get("1");
        assertNotSame(pair, v1);
        assertNotSame(pair, v2);
        assertNotSame(v1, v2);

        assertNotSame(pair, rv1);
        assertNotSame(pair, rv2);
        assertNotSame(rv1, rv2);

        assertTrue(objectMap2.containsValue(v1));
    }

    @Test
    public void countDeserializationsOnContainsValue() {
        final Config config = new Config()
                .addMapConfig(new MapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT));
        final HecatonInstance hz = createHecatonInstance(config);
        final PartitionService partitionService = hz.getPartitionService();
        final IMap<Integer, Object> m = hz.getMap("mappy");
        final HashSet<Integer> nonEmptyPartitions = new HashSet<>();

        for (int i = 0; i < MemberPartitionStateImpl.DEFAULT_PARTITION_COUNT * 5; i++) {
            m.put(i, i);
            nonEmptyPartitions.add(partitionService.getPartition(i).getPartitionId());
        }

        final SerializationCounting value = new SerializationCounting();
        m.containsValue(value);

        assertEquals(nonEmptyPartitions.size(), SerializationCounting.deserializationCount.get());
    }

    public static class SerializationCounting implements DataSerializable {
        public static AtomicInteger serializationCount = new AtomicInteger();
        public static AtomicInteger deserializationCount = new AtomicInteger();

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            serializationCount.incrementAndGet();
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            deserializationCount.incrementAndGet();
        }
    }

    public static final class Pair implements Serializable {

        private final String significant;
        private final String insignificant;

        Pair(String significant, String insignificant) {
            this.significant = significant;
            this.insignificant = insignificant;
        }

        @Override
        public boolean equals(Object thatObj) {
            if (this == thatObj) {
                return true;
            }
            if (thatObj == null || getClass() != thatObj.getClass()) {
                return false;
            }
            Pair that = (Pair) thatObj;
            return this.significant.equals(that.significant);
        }

        @Override
        public int hashCode() {
            return significant.hashCode();
        }
    }

    public static class SerializationValue implements DataSerializable {

        static AtomicInteger deSerializeCount = new AtomicInteger();

        public SerializationValue() {
        }

        @Override
        public void writeData(final ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(final ObjectDataInput in) throws IOException {
            deSerializeCount.incrementAndGet();
        }
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testNativeIMap_throwsException() {
        Config config = getConfig();
        config.getMapConfig("default").setInMemoryFormat(InMemoryFormat.NATIVE);

        HecatonInstance member = createHecatonInstance(config);
        member.getMap("default");
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testNativeNearCache_throwsException() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.NATIVE);

        Config config = getConfig();
        config.getMapConfig("default").setNearCacheConfig(nearCacheConfig);

        HecatonInstance member = createHecatonInstance(config);
        member.getMap("default");
    }
}
