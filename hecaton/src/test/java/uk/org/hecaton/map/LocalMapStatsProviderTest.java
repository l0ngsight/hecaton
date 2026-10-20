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
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.management.ManagementCenterService;
import uk.org.hecaton.internal.metrics.MetricDescriptor;
import uk.org.hecaton.internal.metrics.MetricsRegistry;
import uk.org.hecaton.internal.metrics.collectors.MetricsCollector;
import uk.org.hecaton.map.impl.LocalMapStatsProvider;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalMapStatsProviderTest extends HecatonTestSupport {
    private static final String MAP_NAME = "test_map_1";
    private final TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    //https://github.com/hazelcast/hazelcast/issues/11598
    @Test
    public void testRedundantPartitionMigrationWhenManagementCenterConfigured() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();

        //don't need start management center, just configure it
        final HecatonInstance instance = factory.newHecatonInstance(config);

        assertTrueEventually(() -> {
            ManagementCenterService mcs = getNode(instance).getManagementCenterService();
            assertNotNull(mcs);
        });

        assertTrueAllTheTime(() -> {
            ManagementCenterService mcs = getNode(instance).getManagementCenterService();
            mcs.getTimedMemberStateJson();

            //check partition migration triggered or not
            long partitionStateStamp = getNode(instance).getPartitionService().getPartitionStateStamp();
            assertEquals(0, partitionStateStamp);
        }, 5);
    }

    @Test
    public void testLiteMembersProvideLocalMapStatsForInvocations() {
        // Create a regular member which will own partitions and execute MapOperations
        Config nonLiteConfig = createMetricsBasedConfig();
        HecatonInstance hzFull = factory.newHecatonInstance(nonLiteConfig);
        // Create a Lite member which will only invoke MapOperations
        Config liteConfig = createMetricsBasedConfig().setLiteMember(true);
        HecatonInstance hzLite = factory.newHecatonInstance(liteConfig);

        waitAllForSafeState(hzFull, hzLite);

        // From the Lite member, access and put data into a map
        IMap<Object, Object> map1 = hzLite.getMap(MAP_NAME);
        map1.put("myKey", "myValue");

        // Confirm 1 putCount metric was collected for the Lite member
        assertMapPutCountMetric(hzLite, 1);
        // Confirm no putCount metric was collected for the full member
        assertMapPutCountMetric(hzFull, 0);

        // Destroy the map and ensure metrics are cleaned up
        map1.destroy();
        MapService mapService = getNode(hzLite).getNodeEngine().getService(MapService.SERVICE_NAME);
        LocalMapStatsProvider statsProvider = mapService.getMapServiceContext().getLocalMapStatsProvider();
        assertFalse(statsProvider.hasLocalMapStatsImpl(MAP_NAME));
    }

    private void assertMapPutCountMetric(HecatonInstance instance, int expected) {
        MetricsRegistry metricsRegistry = getNode(instance).getNodeEngine().getMetricsRegistry();
        MapPutCountMetricsCollector metricsCollector = new MapPutCountMetricsCollector();
        metricsRegistry.collect(metricsCollector);
        assertEquals(expected, metricsCollector.totalCollected.get());
    }

    private Config createMetricsBasedConfig() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.setClusterName("dev");
        config.getMetricsConfig().setEnabled(true);
        MapConfig map = config.getMapConfig(MAP_NAME);
        map.setStatisticsEnabled(true);
        map.setPerEntryStatsEnabled(true);
        return config;
    }

    private static class MapPutCountMetricsCollector implements MetricsCollector {
        final AtomicInteger totalCollected = new AtomicInteger();

        @Override
        public void collectLong(MetricDescriptor descriptor, long value) {
            if (descriptor.metric().equals("putCount")) {
                String discriminator = descriptor.discriminatorValue();
                String prefix = descriptor.prefix();
                if (prefix != null && prefix.equals("map") && discriminator != null && discriminator.equals(MAP_NAME)) {
                    totalCollected.addAndGet((int) value);
                }
            }
        }

        @Override
        public void collectDouble(MetricDescriptor descriptor, double value) {
        }

        @Override
        public void collectException(MetricDescriptor descriptor, Exception e) {
        }

        @Override
        public void collectNoValue(MetricDescriptor descriptor) {
        }
    }
}
