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

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.monitor.impl.LocalIndexStatsImpl;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.partition.MigrationListener;
import uk.org.hecaton.partition.MigrationState;
import uk.org.hecaton.partition.ReplicaMigrationEvent;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.jupiter.api.Test;

import javax.management.ObjectName;
import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.MAP_METRIC_INDEX_PARTITIONS_INDEXED;
import static uk.org.hecaton.map.metrics.MetricTestUtils.assertAttributeEquals;
import static uk.org.hecaton.map.metrics.MetricTestUtils.buildMapIndexMetricName;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@ParallelJVMTest
@QuickTest
class PartitionsIndexedTest
        extends HecatonTestSupport {

    @Test
    void testWithGracefulShutdown() {
        testImpl(true);
    }

    @Test
    void testWithUngracefulShutdown() {
        testImpl(false);
    }

    void testImpl(boolean shutdownGracefully) {
        Config config = MetricTestUtils.setRapidMetricsCollection(smallInstanceConfig()).addMapConfig(
                new MapConfig().setName("testMap").addIndexConfig(User.getIndexConfigOnUserId()));

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        HecatonInstance hz0 = factory.newHecatonInstance(config.setInstanceName("main"));
        getTestMap(hz0).put(0, new User("abc", "xyz"));
        assertPartitionsIndexedMatchesPartitionsOwned(hz0);
        CountDownLatch scaleUpMigrationLatch = getMigrationLatch(hz0);
        HecatonInstance hz1 = factory.newHecatonInstance(config.setInstanceName("other"));

        try {
            if (!scaleUpMigrationLatch.await(30, SECONDS)) {
                fail("Timed out waiting for scale up migration");
            }
            assertPartitionsIndexedMatchesPartitionsOwned(hz0);
            assertPartitionsIndexedMatchesPartitionsOwned(hz1);
            CountDownLatch scaleDownMigrationLatch = getMigrationLatch(hz0);
            if (shutdownGracefully) {
                hz1.getLifecycleService().shutdown();
            } else {
                hz1.getLifecycleService().terminate();
            }
            if (!scaleDownMigrationLatch.await(30, SECONDS)) {
                fail("Timed out waiting for scale down migration");
            }
            assertPartitionsIndexedMatchesPartitionsOwned(hz0);
        } catch (InterruptedException e) {
            fail("Test interrupted");
        }
    }

    void assertPartitionsIndexedMatchesPartitionsOwned(HecatonInstance hz) {
        Member hzAsMember = hz.getCluster().getLocalMember();
        long ownedPartitions = hz.getPartitionService().getPartitions().stream().filter(p -> hzAsMember.equals(p.getOwner()))
                                 .count();
        LocalIndexStatsImpl stats = (LocalIndexStatsImpl) getTestMap(hz).getLocalMapStats().getIndexStats().get("testIndex");
        // Even though migration was reported as finished migration finalization (including index building) may still
        // be ongoing so we allow time for it to complete.
        assertTrueEventually(() -> assertThat(stats.getPartitionsIndexed()).isEqualTo(ownedPartitions));
        ObjectName metricName = buildMapIndexMetricName(hz, "testMap", "testIndex");
        assertAttributeEquals(ownedPartitions, metricName, MAP_METRIC_INDEX_PARTITIONS_INDEXED);
    }

    CountDownLatch getMigrationLatch(HecatonInstance hz) {
        CountDownLatch latch = new CountDownLatch(1);
        hz.getPartitionService().addMigrationListener(new MigrationListener() {
            @Override
            public void migrationStarted(MigrationState state) {
            }

            @Override
            public void migrationFinished(MigrationState state) {
                latch.countDown();
            }

            @Override
            public void replicaMigrationCompleted(ReplicaMigrationEvent event) {
            }

            @Override
            public void replicaMigrationFailed(ReplicaMigrationEvent event) {
            }
        });
        return latch;
    }

    IMap<Integer, User> getTestMap(HecatonInstance hz) {
        return hz.getMap("testMap");
    }
}
