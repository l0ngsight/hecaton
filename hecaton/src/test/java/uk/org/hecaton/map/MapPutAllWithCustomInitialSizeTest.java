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
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapPutAllWithCustomInitialSizeTest extends HecatonTestSupport {

    private TestHecatonInstanceFactory factory;
    private HecatonInstance[] instances;

    @Before
    public void setUp() {
        Config config = getConfig()
                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "2")
                .setProperty(ClusterProperty.OPERATION_CALL_TIMEOUT_MILLIS.getName(), "3000")
                .setProperty("hecaton.map.put.all.initial.size.factor", "10");

        config.getMapConfig("default")
                .setBackupCount(1)
                .setAsyncBackupCount(0);

        factory = createHecatonInstanceFactory(1);
        instances = factory.newInstances(config);
        warmUpPartitions(instances);
    }

    @Test
    public void testPutAll() {
        testPutAll(20);
    }

    @Test
    public void testPutAll_withSingleMapEntry() {
        testPutAll(1);
    }

    @Test
    public void testPutAll_withEmptyMap() {
        testPutAll(0);
    }

    private void testPutAll(int expectedEntryCount) {
        String mapName = randomMapName();
        HecatonInstance hz = instances[0];

        Map<Integer, Integer> inputMap = new HashMap<>(expectedEntryCount);
        for (int i = 0; i < expectedEntryCount; i++) {
            inputMap.put(i, i);
        }

        // assert that the map is empty
        IMap<Integer, Integer> map = hz.getMap(mapName);
        assertEquals("Expected an empty map", 0, map.size());

        map.putAll(inputMap);

        // assert that all entries have been written
        assertEquals(format("Expected %d entries in the map", expectedEntryCount), expectedEntryCount, map.size());
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            assertEquals("Expected that key and value are the same", entry.getKey(), entry.getValue());
        }
    }
}
