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

package uk.org.hecaton.map.impl.record;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.query.SampleTestObjects;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Uses basic map functionality with different configuration than existing ones.
 * Actually, it tests {@link ObjectRecordWithStats#getValue()} and {@link ObjectRecordWithStats#setValue(Object)}.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ObjectRecordTest extends HecatonTestSupport {

    private IMap<Integer, Object> map;

    @Before
    public void setup() {
        String mapName = randomMapName();
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        mapConfig.setStatisticsEnabled(false);
        HecatonInstance instance = createHecatonInstance(config);
        map = instance.getMap(mapName);
    }

    @Test
    public void testGetValue() {
        map.put(1, new SampleTestObjects.Employee("alex", 26, true, 25));
        map.get(1);
        assertSizeEventually(1, map);
    }

    @Test
    public void testSetValue() {
        map.put(1, new SampleTestObjects.Employee("alex", 26, true, 25));
        map.put(1, new SampleTestObjects.Employee("tom", 24, true, 10));
        assertSizeEventually(1, map);
    }
}
