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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.MulticastConfig;
import uk.org.hecaton.config.NetworkConfig;
import uk.org.hecaton.config.TcpIpConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonInstanceFactory;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.internal.util.RuntimeAvailableProcessors;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

import static uk.org.hecaton.spi.properties.ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS;
import static uk.org.hecaton.spi.properties.ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS;
import static uk.org.hecaton.spi.properties.ClusterProperty.TCP_JOIN_PORT_TRY_COUNT;
import static uk.org.hecaton.spi.properties.ClusterProperty.WAIT_SECONDS_BEFORE_JOIN;
import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonSerialClassRunner.class)
@Category(NightlyTest.class)
public class JoinStressTest extends HecatonTestSupport {

    private static final long TEN_MINUTES_IN_MILLIS = 10 * 60 * 1000L;
    private ILogger logger = Logger.getLogger(JoinStressTest.class);

    @Before
    @After
    public void tearDown() throws Exception {
        HecatonInstanceFactory.terminateAll();
    }

    @Test(timeout = TEN_MINUTES_IN_MILLIS)
    public void testTCPIPJoinWithManyNodes() throws InterruptedException {
        testJoinWithManyNodes(false);
    }

    @Test(timeout = TEN_MINUTES_IN_MILLIS)
    public void testMulticastJoinWithManyNodes() throws InterruptedException {
        testJoinWithManyNodes(true);
    }

    @Test(timeout = TEN_MINUTES_IN_MILLIS)
    public void testJoinCompletesCorrectlyWhenMultipleNodesStartedParallel() {
        int count = 10;
        final TestHecatonInstanceFactory factory = new TestHecatonInstanceFactory(count);
        final HecatonInstance[] instances = new HecatonInstance[count];
        final CountDownLatch latch = new CountDownLatch(count);

        for (int i = 0; i < count; i++) {
            final int index = i;
            new Thread(() -> {
                instances[index] = factory.newHecatonInstance(createConfig());
                latch.countDown();
            }).start();
        }

        assertOpenEventually(latch);
        for (int i = 0; i < count; i++) {
            assertClusterSizeEventually(count, instances[i]);
        }
    }

    public void testJoinWithManyNodes(final boolean multicast) throws InterruptedException {
        final int nodeCount = 20;
        final int basePort = 12301;
        final CountDownLatch latch = new CountDownLatch(nodeCount);
        final AtomicReferenceArray<HecatonInstance> instances = new AtomicReferenceArray<>(nodeCount);

        ExecutorService ex = Executors.newFixedThreadPool(RuntimeAvailableProcessors.get() * 2);
        for (int i = 0; i < nodeCount; i++) {
            final int portSeed = i;
            ex.execute(() -> {
                sleepRandom(1, 1000);

                Config config = createConfig();
                initNetworkConfig(config.getNetworkConfig(), basePort, portSeed, multicast, nodeCount);

                HecatonInstance h = Hecaton.newHecatonInstance(config);
                instances.set(portSeed, h);
                latch.countDown();
            });
        }

        try {
            latch.await(200, TimeUnit.SECONDS);
        } finally {
            ex.shutdown();
        }

        for (int i = 0; i < nodeCount; i++) {
            HecatonInstance hz = instances.get(i);
            assertNotNull(hz);
            logEvaluatedMember(hz);
            assertClusterSizeEventually(nodeCount, hz);
        }
    }

    private Config createConfig() {
        Config config = new Config();
        config.setProperty(MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "3");
        config.setProperty(MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3");
        return config;
    }

    private static void sleepRandom(int min, int max) {
        int rand = (int) (Math.random() * (max - min)) + min;
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(rand));
    }

    @Test(timeout = TEN_MINUTES_IN_MILLIS)
    public void testTCPIPJoinWithManyNodesMultipleGroups() throws InterruptedException {
        testJoinWithManyNodesMultipleGroups(false);
    }

    @Test(timeout = TEN_MINUTES_IN_MILLIS)
    public void testMulticastJoinWithManyNodesMultipleGroups() throws InterruptedException {
        testJoinWithManyNodesMultipleGroups(true);
    }

    private void testJoinWithManyNodesMultipleGroups(final boolean multicast) throws InterruptedException {
        final int nodeCount = 10;
        final int groupCount = 3;
        final int basePort = 12301;
        final CountDownLatch latch = new CountDownLatch(nodeCount);
        final AtomicReferenceArray<HecatonInstance> instances = new AtomicReferenceArray<>(nodeCount);

        final Map<String, AtomicInteger> groups = new HashMap<>(groupCount);
        for (int i = 0; i < groupCount; i++) {
            groups.put("group-" + i, new AtomicInteger());
        }

        ExecutorService ex = Executors.newFixedThreadPool(RuntimeAvailableProcessors.get() * 2);
        for (int i = 0; i < nodeCount; i++) {
            final int portSeed = i;
            ex.execute(() -> {
                sleepRandom(1, 1000);

                Config config = createConfig();
                String name = "group-" + (int) (Math.random() * groupCount);
                config.setClusterName(name);

                groups.get(name).incrementAndGet();

                initNetworkConfig(config.getNetworkConfig(), basePort, portSeed, multicast, nodeCount);

                HecatonInstance h = Hecaton.newHecatonInstance(config);
                instances.set(portSeed, h);
                latch.countDown();
            });
        }
        try {
            latch.await(200, TimeUnit.SECONDS);
        } finally {
            ex.shutdown();
        }

        for (int i = 0; i < nodeCount; i++) {
            HecatonInstance hz = instances.get(i);
            assertNotNull(hz);
            logEvaluatedMember(hz);

            final String clusterName = hz.getConfig().getClusterName();
            final int shouldBeClusterSize = groups.get(clusterName).get();
            assertTrueEventually(() -> {
                assertEquals(clusterName + ": ", shouldBeClusterSize, hz.getCluster().getMembers().size());
            });
        }
    }

    private void logEvaluatedMember(HecatonInstance hz) {
        ClusterServiceImpl instanceClusterService = getNode(hz).getClusterService();
        logger.info("Evaluating member: " + hz + " " + hz.getLocalEndpoint().getSocketAddress() + " with memberList "
                + instanceClusterService.getMemberListString());
    }

    private void initNetworkConfig(NetworkConfig networkConfig, int basePort, int portSeed, boolean multicast, int nodeCount) {
        networkConfig.setPortAutoIncrement(false);
        networkConfig.setPort(basePort + portSeed);

        JoinConfig join = networkConfig.getJoin();

        MulticastConfig multicastConfig = join.getMulticastConfig();
        multicastConfig.setEnabled(multicast);
        multicastConfig.setMulticastTimeoutSeconds(5);

        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        tcpIpConfig.setEnabled(!multicast);
        tcpIpConfig.setConnectionTimeoutSeconds(5);

        List<String> members = new ArrayList<>(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            members.add("127.0.0.1:" + (basePort + i));
        }
        Collections.sort(members);
        tcpIpConfig.setMembers(members);
    }

    @Test(timeout = 300000)
    public void testJoinWhenMemberClosedInBetween() throws InterruptedException {
        //Test is expecting to all can join safely.
        // On the failed case the last opened instance throws java.lang.IllegalStateException: Node failed to start!
        Config config = new Config();
        HecatonInstance i1 = Hecaton.newHecatonInstance(config);
        HecatonInstance i2 = Hecaton.newHecatonInstance(config);
        HecatonInstance i3 = Hecaton.newHecatonInstance(config);
        HecatonInstance i4 = Hecaton.newHecatonInstance(config);

        final IMap<Integer, Integer> map = i4.getMap("a");
        int numThreads = 40;
        final int loop = 5000;

        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                Random random = new Random();
                for (int j = 0; j < loop; j++) {
                    int op = random.nextInt(3);
                    if (op == 0) {
                        map.put(j, j);
                    } else if (op == 1) {
                        Integer val = map.remove(j);
                        assert val == null || val.equals(j);
                    } else {
                        Integer val = map.get(j);
                        assert val == null || val.equals(j);
                    }

                }
            });
            threads[i].start();
        }

        i1.shutdown();
        i2.shutdown();
        i3.shutdown();

        //Should not throw java.lang.IllegalStateException: Node failed to start!
        Hecaton.newHecatonInstance(config);

        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }
    }

    @Test
    public void testTcpJoin_whenInitialMembersTerminated_duringStartup() throws Exception {
        testJoin_whenInitialMembersTerminated_duringStartup(false);
    }

    @Test
    public void testMulticastJoin_whenInitialMembersTerminated_duringStartup() throws Exception {
        testJoin_whenInitialMembersTerminated_duringStartup(true);
    }

    private void testJoin_whenInitialMembersTerminated_duringStartup(boolean multicast) throws Exception {
        int nodeCount = 5;

        final Config config = new Config();
        config.setProperty(WAIT_SECONDS_BEFORE_JOIN.getName(), WAIT_SECONDS_BEFORE_JOIN.getDefaultValue());
        config.setProperty(ClusterProperty.ASYNC_JOIN_STRATEGY_ENABLED.getName(), "false");
        config.setProperty(TCP_JOIN_PORT_TRY_COUNT.getName(), String.valueOf(nodeCount));
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(multicast);
        join.getTcpIpConfig().setEnabled(!multicast).clear().addMember("127.0.0.1");

        final HecatonInstance[] instances = new HecatonInstance[nodeCount - 1];
        for (int i = 0; i < instances.length; i++) {
            instances[i] = Hecaton.newHecatonInstance(config);
        }

        Callable<HecatonInstance> newInstanceTask = () -> Hecaton.newHecatonInstance(config);

        Future<HecatonInstance> future1 = spawn(newInstanceTask);
        Future<HecatonInstance> future2 = spawn(newInstanceTask);

        spawn(() -> {
            for (HecatonInstance instance : instances) {
                sleepRandom(500, 1000);
                instance.getLifecycleService().terminate();
            }
        });

        assertCompletesEventually(future1);
        assertCompletesEventually(future2);

        HecatonInstance instance1 = future1.get();
        HecatonInstance instance2 = future2.get();
        assertClusterSizeEventually(2, instance1, instance2);
    }
}
