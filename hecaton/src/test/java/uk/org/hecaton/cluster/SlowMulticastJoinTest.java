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

package uk.org.hecaton.cluster;

import uk.org.hecaton.cluster.SplitBrainHandlerTest.MergedEventLifeCycleListener;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.internal.cluster.impl.MulticastJoiner;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category(NightlyTest.class)
public class SlowMulticastJoinTest extends AbstractJoinTest {

    @Before
    @After
    public void killAllHecatonInstances() {
        HecatonInstanceFactory.terminateAll();
    }

    @Test
    public void testMembersStaysIndependentWhenHostIsNotTrusted() {
        Config config1 = newConfig("8.8.8.8"); //8.8.8.8 is never a local address
        Config config2 = newConfig("8.8.8.8");

        int testDurationSeconds = 30;
        assertIndependentClustersAndDoNotMergedEventually(config1, config2, testDurationSeconds);
    }

    @Test
    public void testMembersFormAClusterWhenHostIsTrusted() {
        Config config2 = newConfig("*.*.*.*"); //matching everything

        testJoin(config2);
    }

    @Test
    public void testSplitBrainMessagesNotAccumulated_whenClusterIsStableOrNodeIsNotMaster() throws Exception {
        final int clusterSize = 3;
        Config config = new Config();
        config.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "10");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "15");

        final HecatonInstance[] instances = new HecatonInstance[clusterSize];
        final MulticastJoiner[] joiners = new MulticastJoiner[clusterSize];

        for (int i = 0; i < clusterSize; i++) {
            instances[i] = Hecaton.newHecatonInstance(config);
            joiners[i] = (MulticastJoiner) getNode(instances[i]).getJoiner();
        }

        assertClusterSize(clusterSize, instances[0]);

        // we will split the cluster to subclusters (0, 1), (2)
        final CountDownLatch splitLatch = new CountDownLatch(2);
        instances[2].getCluster().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                splitLatch.countDown();
            }

        });

        final CountDownLatch mergeLatch = new CountDownLatch(1);
        instances[2].getLifecycleService().addLifecycleListener(new MergedEventLifeCycleListener(mergeLatch));

        // while cluster is stable, split brain join messages should not be accumulated.
        // the master member for the 10 seconds duration of assertion may have up to as many as other members
        // in the cluster (clusterSize-1) messages in its queue which will be ignored by the SplitBrainHandler.
        assertSplitBrainMessagesCount(clusterSize, instances, joiners);

        // split cluster
        closeConnectionBetween(instances[0], instances[2]);
        closeConnectionBetween(instances[1], instances[2]);

        assertTrue(splitLatch.await(10, TimeUnit.SECONDS));

        // while cluster is split, no split brain join messages should be accumulated in the non-master member 1
        assertSplitBrainMessagesCount(clusterSize, new HecatonInstance[]{instances[1]},
                new MulticastJoiner[]{joiners[1]});

        assertTrue(mergeLatch.await(30, TimeUnit.SECONDS));
        assertClusterSize(clusterSize, instances[0]);

        // cluster is merged & stable again, split brain join messages should not be accumulated.
        assertSplitBrainMessagesCount(clusterSize, instances, joiners);
    }

    private void assertSplitBrainMessagesCount(final int clusterSize, final HecatonInstance[] instances,
                                               final MulticastJoiner[] joiners) {
        assertTrueAllTheTime(() -> {
            for (int i = 0; i < instances.length; i++) {
                // a master can have at most (clusterSize-1) split brain join messages
                if (getNode(instances[i]).isMaster()) {
                    assertTrue(joiners[i].getSplitBrainMessagesCount() < clusterSize);
                } else {
                    // other members should not have any split brain join messages
                    assertEquals(0, joiners[i].getSplitBrainMessagesCount());
                }
            }
        }, 10);
    }

    private Config newConfig(String trustedInterface) {
        Config config = new Config();
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3");
        config.getNetworkConfig().getJoin().getMulticastConfig().addTrustedInterface(trustedInterface);
        return config;
    }
}
