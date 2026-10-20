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
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.monitor.impl.LocalMapStatsImpl;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.query.impl.predicates.EqualPredicate;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.jupiter.api.Test;

import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.MAP_METRIC_NO_MATCHING_INDEX_QUERY_COUNT;
import static uk.org.hecaton.map.metrics.MetricTestUtils.assertAttributeEquals;
import static uk.org.hecaton.map.metrics.MetricTestUtils.buildMapMetricName;
import static org.assertj.core.api.Assertions.assertThat;

@ParallelJVMTest
@QuickTest
class NoIndexMatchedQueryCountTest
        extends HecatonTestSupport {

    @Test
    void testCounterIncrementing() {
        Config config = MetricTestUtils.setRapidMetricsCollection(smallInstanceConfig()).addMapConfig(
                new MapConfig().setName("testMap").addIndexConfig(User.getIndexConfigOnUserId()));

        HecatonInstance hz = createHecatonInstance(config);
        IMap<Integer, User> testMap = hz.getMap("testMap");
        testMap.put(0, new User("123", "345"));

        for (int i = 0; i < 5; i++) {
            testMap.entrySet(new EqualPredicate("userId", "" + i));
        }

        for (int i = 0; i < 12; i++) {
            testMap.entrySet(new EqualPredicate("name", "" + i));
        }

        LocalMapStatsImpl stats = (LocalMapStatsImpl) testMap.getLocalMapStats();
        assertThat(stats.getQueryCount()).isEqualTo(17);
        assertThat(stats.getIndexedQueryCount()).isEqualTo(5);
        assertThat(stats.getIndexesSkippedQueryCount()).isZero();

        assertThat(stats.getNoMatchingIndexQueryCount()).isEqualTo(12);
        assertAttributeEquals(12L, buildMapMetricName(hz, "testMap"), MAP_METRIC_NO_MATCHING_INDEX_QUERY_COUNT);
    }
}
