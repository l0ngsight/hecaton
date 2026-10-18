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

package uk.org.hecaton.jet.impl.connector;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.test.TestOutbox;
import uk.org.hecaton.jet.core.test.TestProcessorContext;
import uk.org.hecaton.jet.core.test.TestSupport;
import uk.org.hecaton.map.EventJournalMapEvent;
import uk.org.hecaton.map.impl.proxy.MapProxyImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

import static uk.org.hecaton.jet.core.EventTimePolicy.eventTimePolicy;
import static uk.org.hecaton.jet.core.WatermarkPolicy.limitingLag;
import static uk.org.hecaton.jet.impl.execution.WatermarkCoalescer.IDLE_MESSAGE;
import static uk.org.hecaton.jet.impl.util.Util.uncheckRun;
import static uk.org.hecaton.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_COUNT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StreamEventJournalP_WmCoalescingTest extends JetTestSupport {

    private static final int JOURNAL_CAPACITY = 10;

    private MapProxyImpl<Integer, Integer> map;
    private int[] partitionKeys;
    private HecatonInstance instance;

    @Before
    public void setUp() {
        Config config = smallInstanceConfig();

        String mapName = randomMapName();
        MapConfig mapConfig = new MapConfig();
        mapConfig.setName(mapName);
        mapConfig.getEventJournalConfig()
                 .setCapacity(JOURNAL_CAPACITY)
                 .setEnabled(true);

        config.setProperty(PARTITION_COUNT.getName(), "2");
        config.addMapConfig(mapConfig);
        instance = createHecatonInstance(config);

        map = (MapProxyImpl<Integer, Integer>) instance.<Integer, Integer>getMap(mapName);

        partitionKeys = new int[2];
        for (int i = 1; IntStream.of(partitionKeys).anyMatch(val -> val == 0); i++) {
            int partitionId = instance.getPartitionService().getPartition(i).getPartitionId();
            partitionKeys[partitionId] = i;
        }
    }

    @Test
    public void when_entryInEachPartition_then_wmForwarded() {
        map.put(partitionKeys[0], 10);
        map.put(partitionKeys[1], 10);

        TestSupport.verifyProcessor(createSupplier(asList(0, 1), 5000))
                   .disableProgressAssertion()
                   .runUntilOutputMatches(60_000, 100)
                   .disableSnapshots()
                   .hecatonInstance(instance)
                   .expectOutput(asList(wm(10), 10, 10));
    }

    @Test
    public void when_entryInOnePartition_then_wmForwardedAfterIdleTime() throws InterruptedException {
        // initially, there will be entries in both partitions
        map.put(partitionKeys[0], 11);
        map.put(partitionKeys[1], 11);

        // insert to map in parallel to verifyProcessor so that the partition0 is not marked as idle
        // but partition1 is
        CountDownLatch productionStartedLatch = new CountDownLatch(1);
        Future<?> future = spawn(() -> {
            while (!Thread.interrupted()) {
                LockSupport.parkNanos(MILLISECONDS.toNanos(500));
                map.put(partitionKeys[0], 11);
                productionStartedLatch.countDown();
            }
        });
        productionStartedLatch.await();

        TestSupport.verifyProcessor(createSupplier(asList(0, 1), 5000))
                   .disableProgressAssertion()
                   .runUntilOutputMatches(60_000, 100)
                   .disableSnapshots()
                   .hecatonInstance(instance)
                   .outputChecker((e, a) -> new HashSet<>(e).equals(new HashSet<>(a)))
                   .expectOutput(asList(11, wm(11)));

        future.cancel(true);
    }

    @Test
    public void when_allPartitionsIdle_then_idleMessageOutput() {
        TestSupport.verifyProcessor(createSupplier(asList(0, 1), 500))
                   .disableProgressAssertion()
                   .runUntilOutputMatches(60_000, 100)
                   .disableSnapshots()
                   .hecatonInstance(instance)
                   .expectOutput(singletonList(IDLE_MESSAGE));
    }

    @Test
    public void when_allPartitionsIdleAndThenRecover_then_wmOutput() throws Exception {
        // Insert to map in parallel to verifyProcessor.
        CountDownLatch latch = new CountDownLatch(1);
        Thread updatingThread = new Thread(() -> uncheckRun(() -> {
            // We will start after a delay so that the source will first become idle and then recover.
            latch.await();
            for (; ; ) {
                map.put(partitionKeys[0], 12);
                Thread.sleep(100);
            }
        }));
        updatingThread.start();

        Processor processor = createSupplier(asList(0, 1), 2000).get();
        TestOutbox outbox = new TestOutbox(1024);
        Queue<Object> outbox0 = outbox.queue(0);
        processor.init(outbox, new TestProcessorContext().setHecatonInstance(instance));

        assertTrueEventually(() -> {
            processor.complete();
            // after we have the IDLE_MESSAGE, release the latch to let the other thread produce events
            if (IDLE_MESSAGE.equals(outbox0.peek())) {
                latch.countDown();
            }
            assertEquals(asList(IDLE_MESSAGE, wm(12), 12), outbox0.stream().distinct().collect(toList()));
        });

        updatingThread.interrupt();
        updatingThread.join();
    }

    @Test
    public void test_nonFirstPartition() {
        /* aim of this test is to check that the mapping from partitionIndex to partitionId works */
        map.put(partitionKeys[1], 13);

        TestSupport.verifyProcessor(createSupplier(singletonList(1), 5000))
                   .disableProgressAssertion()
                   .runUntilOutputMatches(60_000, 100)
                   .disableSnapshots()
                   .hecatonInstance(instance)
                   .expectOutput(asList(wm(13), 13, IDLE_MESSAGE));
    }

    public SupplierEx<Processor> createSupplier(List<Integer> assignedPartitions, long idleTimeout) {
        return () -> new StreamEventJournalP<>(map, assignedPartitions, e -> true,
                EventJournalMapEvent::getNewValue, START_FROM_OLDEST, false,
                eventTimePolicy(Integer::intValue, limitingLag(0), 1, 0, idleTimeout));
    }
}
