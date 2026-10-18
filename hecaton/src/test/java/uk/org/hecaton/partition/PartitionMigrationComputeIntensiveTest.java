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

package uk.org.hecaton.partition;

import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static uk.org.hecaton.partition.PartitionMigrationListenerTest.EventCollectingMigrationListener;
import static uk.org.hecaton.partition.PartitionMigrationListenerTest.MigrationEventsPack;
import static uk.org.hecaton.partition.PartitionMigrationListenerTest.assertMigrationEventsConsistentWithResult;
import static uk.org.hecaton.partition.PartitionMigrationListenerTest.assertMigrationProcessCompleted;
import static uk.org.hecaton.partition.PartitionMigrationListenerTest.assertMigrationProcessEventsConsistent;
import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_COUNT;
import static uk.org.hecaton.spi.properties.ClusterProperty.SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED;

@RunWith(HecatonSerialClassRunner.class)
@Category({NightlyTest.class})
public class PartitionMigrationComputeIntensiveTest extends HecatonTestSupport {

    private static final ILogger LOGGER = Logger.getLogger(PartitionMigrationComputeIntensiveTest.class);

    @Test
    public void testMigrationStats_afterPartitionsLost_when_NO_MIGRATION() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        Config config = new Config().setProperty(PARTITION_COUNT.getName(), "2000")
                                    .setProperty(SLOW_OPERATION_DETECTOR_STACK_TRACE_LOGGING_ENABLED.getName(), "true");
        HecatonInstance[] instances = factory.newInstances(config, 10);

        assertClusterSizeEventually(instances.length, instances);

        LOGGER.info("Cluster formed and in safe state, warming up partitions...");
        warmUpPartitions(instances);

        LOGGER.info("Adding migration listener");
        EventCollectingMigrationListener listener = new EventCollectingMigrationListener(false);
        instances[0].getPartitionService().addMigrationListener(listener);

        LOGGER.info("Changing cluster state to PASSIVE");
        changeClusterStateEventually(instances[0], ClusterState.PASSIVE);

        for (int i = 3; i < instances.length; i++) {
            instances[i].getLifecycleService().terminate();
        }

        LOGGER.info("Changing cluster state to NO_MIGRATION");
        changeClusterStateEventually(instances[0], ClusterState.NO_MIGRATION);

        // 3 promotions on each remaining node + 1 to assign owners for lost partitions
        for (MigrationEventsPack eventsPack : listener.ensureAndGetEventPacks(4)) {
            assertMigrationProcessCompleted(eventsPack);
            assertMigrationProcessEventsConsistent(eventsPack);
            assertMigrationEventsConsistentWithResult(eventsPack);
        }
    }
}
