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
package uk.org.hecaton.map.impl.operation;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapChunkTest extends HecatonTestSupport {

    @Test
    public void smoke() {
        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance node1 = factory.newHecatonInstance(config);
        IMap test = node1.getMap("test");
        for (int i = 0; i < 1_000; i++) {
            test.set(i, i);
        }
        HecatonInstance node2 = factory.newHecatonInstance(config);

        node1.shutdown();

        assertEquals(1_000, node2.getMap("test").size());
    }

    @Test
    public void smoke_multiple_map() {
        Config config = getConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

        HecatonInstance node1 = factory.newHecatonInstance(config);
        for (int j = 0; j < 10; j++) {
            IMap test = node1.getMap("test-" + j);
            for (int i = 0; i < 1_000; i++) {
                test.set(i, i);
            }
        }
        HecatonInstance node2 = factory.newHecatonInstance(config);

        node1.shutdown();

        for (int j = 0; j < 10; j++) {
            IMap test = node2.getMap("test-" + j);
            assertEquals(1_000, test.size());
        }
    }
}
