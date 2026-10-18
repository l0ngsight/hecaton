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

package uk.org.hecaton.internal.partition;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.TestUtil;
import uk.org.hecaton.internal.util.RandomPicker;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.test.Accessors.getAddress;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * https://github.com/hazelcast/hazelcast/issues/5444
 */
@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category({SlowTest.class})
@Ignore(value = "https://github.com/hazelcast/hazelcast/issues/9828")
public class SlowMigrationCorrectnessTest extends AbstractMigrationCorrectnessTest {

    @Parameters(name = "backups:{0},nodes:{1},fragmented:{2}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {1, 2, true},
                {1, InternalPartition.MAX_REPLICA_COUNT, true},
                {2, 3, true},
                {2, InternalPartition.MAX_REPLICA_COUNT, true},
                {3, 4, true},
                {3, InternalPartition.MAX_REPLICA_COUNT, true},
                {3, 4, false},
                {3, InternalPartition.MAX_REPLICA_COUNT, false},
        });
    }

    @Test(timeout = 6000 * 10 * 10)
    public void testPartitionData_whenSameNodesRestarted_afterPartitionsSafe() throws InterruptedException {
        partitionCount = 14;

        Config config = getConfig(true, false);
        config.setProperty(ClusterProperty.PARTITION_MIGRATION_INTERVAL.getName(), "1");

        HecatonInstance[] instances = factory.newInstances(config, nodeCount);
        warmUpPartitions(instances);

        fillData(instances[instances.length - 1]);
        assertSizeAndDataEventually();

        Address[] restartingAddresses = new Address[backupCount];
        for (int i = 0; i < backupCount; i++) {
            restartingAddresses[i] = getAddress(instances[i]);
        }

        for (int i = 0; i < 5; i++) {
            restartNodes(config, restartingAddresses);
        }
        assertSizeAndDataEventually();
    }

    private void restartNodes(final Config config, Address[] addresses) throws InterruptedException {
        if (addresses.length == 1) {
            HecatonInstance hz = factory.getInstance(addresses[0]);
            assertNotNull("No instance known for address: " + addresses[0], hz);
            TestUtil.terminateInstance(hz);
            sleepMillis(RandomPicker.getInt(1, 3000));
            factory.newHecatonInstance(addresses[0], config);
        } else {
            final CountDownLatch latch = new CountDownLatch(addresses.length);
            for (final Address address : addresses) {
                final HecatonInstance hz = factory.getInstance(address);
                assertNotNull("No instance known for address: " + address, hz);

                new Thread(() -> {
                    TestUtil.terminateInstance(hz);
                    sleepMillis(RandomPicker.getInt(1, 3000));
                    factory.newHecatonInstance(address, config);
                    latch.countDown();
                }).start();
            }
            assertTrue(latch.await(2, TimeUnit.MINUTES));
        }
    }
}
