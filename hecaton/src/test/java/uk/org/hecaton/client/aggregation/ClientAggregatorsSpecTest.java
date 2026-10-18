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

import uk.org.hecaton.aggregation.AggregatorsSpecTest;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import static uk.org.hecaton.spi.properties.ClusterProperty.AGGREGATION_ACCUMULATION_PARALLEL_EVALUATION;
import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_COUNT;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientAggregatorsSpecTest extends AggregatorsSpecTest {

    private TestHecatonFactory factory;

    @Override
    protected <K, V> IMap<K, V> getMapWithNodeCount(int nodeCount, boolean parallelAccumulation, boolean useIndex) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("node count < 1");
        }

        MapConfig mapConfig = new MapConfig()
                .setName("aggr")
                .setInMemoryFormat(inMemoryFormat);

        if (useIndex) {
            mapConfig.addIndexConfig(new IndexConfig(IndexConfig.DEFAULT_TYPE, "fieldWeCanQuery"));
        }

        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), String.valueOf(nodeCount))
                .setProperty(AGGREGATION_ACCUMULATION_PARALLEL_EVALUATION.getName(), String.valueOf(parallelAccumulation))
                .addMapConfig(mapConfig);

        factory = new TestHecatonFactory();
        for (int i = 0; i < nodeCount; i++) {
            factory.newHecatonInstance(config);
        }
        HecatonInstance instance = factory.newHecatonClient();
        return instance.getMap("aggr");
    }

    @After
    public void teardown() {
        factory.terminateAll();
    }
}
