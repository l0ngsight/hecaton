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

package uk.org.hecaton.map.metrics;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.monitor.impl.PerIndexStats;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.query.impl.IndexCopyBehavior;
import uk.org.hecaton.query.impl.IndexRegistry;
import uk.org.hecaton.query.impl.IndexUtils;
import uk.org.hecaton.query.impl.InternalIndex;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.jupiter.api.Test;

import javax.management.ObjectName;

import static uk.org.hecaton.config.MapConfig.DEFAULT_IN_MEMORY_FORMAT;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.MAP_METRIC_INDEX_PARTITION_UPDATES_FINISHED;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.MAP_METRIC_INDEX_PARTITION_UPDATES_STARTED;
import static uk.org.hecaton.map.metrics.MetricTestUtils.assertAttributeEquals;
import static uk.org.hecaton.map.metrics.MetricTestUtils.buildMapIndexMetricName;
import static org.assertj.core.api.Assertions.assertThat;

@ParallelJVMTest
@QuickTest
class PartitionUpdateCountersTest
        extends HecatonTestSupport {

    @Test
    void testCountersOnPublishedMetric() {
        Config config = MetricTestUtils.setRapidMetricsCollection(smallInstanceConfig()).addMapConfig(
                new MapConfig().setName("testMap").addIndexConfig(User.getIndexConfigOnUserId()));

        HecatonInstance hz = createHecatonInstance(config);
        IMap<Integer, User> testMap = hz.getMap("testMap");
        for (int i = 0; i < 10; i++) {
            testMap.put(i, new User("" + i, ""));
        }
        long partitionCount = hz.getPartitionService().getPartitions().size();
        ObjectName indexName = buildMapIndexMetricName(hz, "testMap", "testIndex");
        assertAttributeEquals(partitionCount, indexName, MAP_METRIC_INDEX_PARTITION_UPDATES_STARTED);
        assertAttributeEquals(partitionCount, indexName, MAP_METRIC_INDEX_PARTITION_UPDATES_FINISHED);
    }

    @Test
    void testCountersOnPartitionIndexing() {
        testCounterOnPartitionChange(true);
    }

    @Test
    void testCountersOnPartitionUnindexing() {
        testCounterOnPartitionChange(false);
    }

    void testCounterOnPartitionChange(boolean indexing) {
        IndexRegistry indexRegistry = IndexRegistry.newBuilder(null, "test", new DefaultSerializationServiceBuilder().build(),
                IndexCopyBehavior.NEVER, DEFAULT_IN_MEMORY_FORMAT).global(true).statsEnabled(true).build();

        InternalIndex index = indexRegistry.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "abc"));
        PerIndexStats stats = index.getPerIndexStats();
        int changeCount = 5;
        for (int i = 0; i < changeCount; i++) {
            index.beginPartitionUpdate();
            assertThat(stats.getPartitionUpdatesStarted()).isEqualTo(i + 1);
            assertThat(stats.getPartitionUpdatesFinished()).isZero();
        }
        for (int i = 0; i < changeCount; i++) {
            if (indexing) {
                index.markPartitionAsIndexed(i);
            } else {
                index.markPartitionAsUnindexed(i);
            }
            assertThat(stats.getPartitionUpdatesStarted()).isEqualTo(5);
            assertThat(stats.getPartitionUpdatesFinished()).isEqualTo(i + 1);
        }
    }
}
