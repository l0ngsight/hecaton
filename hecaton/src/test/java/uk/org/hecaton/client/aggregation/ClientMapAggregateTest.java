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

package uk.org.hecaton.client.aggregation;

import uk.org.hecaton.aggregation.MapAggregateTest;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapAggregateTest
        extends MapAggregateTest {

    private TestHecatonFactory factory;
    private HecatonInstance client;

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Override
    public <K, V> IMap<K, V> getMapWithNodeCount(int nodeCount) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("node count < 1");
        }

        factory = new TestHecatonFactory();

        Config config = new Config();
        config.setProperty("hecaton.partition.count", "3");
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName("aggr");
        mapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        config.addMapConfig(mapConfig);

        factory.newInstances(config, nodeCount);
        client = factory.newHecatonClient();

        return client.getMap("aggr");
    }

}
