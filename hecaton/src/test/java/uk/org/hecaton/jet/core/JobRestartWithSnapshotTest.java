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

package uk.org.hecaton.jet.core;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.Traverser;
import uk.org.hecaton.jet.Traversers;
import uk.org.hecaton.jet.accumulator.LongAccumulator;
import uk.org.hecaton.jet.aggregate.AggregateOperation1;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.core.processor.Processors;
import uk.org.hecaton.jet.core.processor.SinkProcessors;
import uk.org.hecaton.jet.datamodel.KeyedWindowResult;
import uk.org.hecaton.jet.impl.JobExecutionRecord;
import uk.org.hecaton.jet.impl.JobRepository;
import uk.org.hecaton.jet.impl.execution.init.JetInitDataSerializerHook;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static uk.org.hecaton.function.Functions.entryKey;
import static uk.org.hecaton.jet.Util.entry;
import static uk.org.hecaton.jet.aggregate.AggregateOperations.counting;
import static uk.org.hecaton.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static uk.org.hecaton.jet.core.BroadcastKey.broadcastKey;
import static uk.org.hecaton.jet.core.Edge.between;
import static uk.org.hecaton.jet.core.EventTimePolicy.eventTimePolicy;
import static uk.org.hecaton.jet.core.TestUtil.throttle;
import static uk.org.hecaton.jet.core.WatermarkPolicy.limitingLag;
import static uk.org.hecaton.jet.core.processor.Processors.combineToSlidingWindowP;
import static uk.org.hecaton.jet.core.processor.Processors.insertWatermarksP;
import static uk.org.hecaton.jet.core.processor.Processors.mapP;
import static uk.org.hecaton.jet.core.processor.SinkProcessors.writeListP;
import static uk.org.hecaton.jet.impl.util.Util.arrayIndexOf;
import static uk.org.hecaton.test.PacketFiltersUtil.delayOperationsFrom;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class JobRestartWithSnapshotTest extends JetTestSupport {

    private static final int LOCAL_PARALLELISM = 4;

    private HecatonInstance instance1;
    private HecatonInstance instance2;

    @Before
    public void setup() {
        Config config = smallInstanceConfig();
        config.getJetConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);

        instance1 = createHecatonInstance(config);
        instance2 = createHecatonInstance(config);
    }

    @Test
    public void when_nodeDown_then_jobRestartsFromSnapshot_singleStage() throws Exception {
        when_nodeDown_then_jobRestartsFromSnapshot(false);
    }

    @Test
    public void when_nodeDown_then_jobRestartsFromSnapshot_twoStage() throws Exception {
        when_nodeDown_then_jobRestartsFromSnapshot(true);
    }

    @SuppressWarnings("unchecked")
    private void when_nodeDown_then_jobRestartsFromSnapshot(boolean twoStage) throws Exception {
        /*
        Design of this test:

        It uses a random partitioned generator of source events. The events are
        Map.Entry(partitionId, timestamp). For each partition timestamps from
        0..elementsInPartition are generated.

        We start the test with two nodes and localParallelism(1) and 3 partitions
        for source. Source instances generate items at the same rate of 10 per
        second: this causes one instance to be twice as fast as the other in terms of
        timestamp. The source processor saves partition offsets similarly to how
        KafkaSources.kafka() and Sources.mapJournal() do.

        After some time we shut down one instance. The job restarts from the
        snapshot and all partitions are restored to single source processor
        instance. Partition offsets are very different, so the source is written
        in a way that it emits from the most-behind partition in order to not
        emit late events from more ahead partitions.

        Local parallelism of InsertWatermarkP is also 1 to avoid the edge case
        when different instances of InsertWatermarkP might initialize with first
        event in different frame and make them start the no-gap emission from
        different WM, which might cause the SlidingWindowP downstream to miss
        some of the first windows.

        The sink writes to an IMap which is an idempotent sink.

        The resulting contents of the sink map are compared to expected value.
        */

        DAG dag = new DAG();

        SlidingWindowPolicy wDef = SlidingWindowPolicy.tumblingWinPolicy(3);
        AggregateOperation1<Object, LongAccumulator, Long> aggrOp = counting();

        IMap<List<Long>, Long> result = instance1.getMap("result");
        result.clear();

        int numPartitions = 3;
        int elementsInPartition = 250;
        SupplierEx<Processor> sup = () ->
                new SequencesInPartitionsGeneratorP(numPartitions, elementsInPartition, true);
        Vertex generator = dag.newVertex("generator", throttle(sup, 30))
                              .localParallelism(1);
        Vertex insWm = dag.newVertex("insWm", insertWatermarksP(eventTimePolicy(
                o -> ((Entry<Integer, Integer>) o).getValue(), limitingLag(0), wDef.frameSize(), wDef.frameOffset(), 0)))
                          .localParallelism(1);
        Vertex map = dag.newVertex("map",
                mapP((KeyedWindowResult kwr) -> entry(asList(kwr.end(), (long) (int) kwr.key()), kwr.result())));
        Vertex writeMap = dag.newVertex("writeMap", SinkProcessors.writeMapP("result"));

        if (twoStage) {
            Vertex aggregateStage1 = dag.newVertex("aggregateStage1", Processors.accumulateByFrameP(
                    singletonList((FunctionEx<? super Object, ?>) t -> ((Entry<Integer, Integer>) t).getKey()),
                    singletonList(t1 -> ((Entry<Integer, Integer>) t1).getValue()),
                    TimestampKind.EVENT,
                    wDef,
                    aggrOp.withIdentityFinish()
            ));
            Vertex aggregateStage2 = dag.newVertex("aggregateStage2",
                    combineToSlidingWindowP(wDef, aggrOp, KeyedWindowResult::new));

            dag.edge(between(insWm, aggregateStage1)
                    .partitioned(entryKey()))
               .edge(between(aggregateStage1, aggregateStage2)
                       .distributed()
                       .partitioned(entryKey()))
               .edge(between(aggregateStage2, map));
        } else {
            Vertex aggregate = dag.newVertex("aggregate", Processors.aggregateToSlidingWindowP(
                    singletonList((FunctionEx<Object, Integer>) t -> ((Entry<Integer, Integer>) t).getKey()),
                    singletonList(t1 -> ((Entry<Integer, Integer>) t1).getValue()),
                    TimestampKind.EVENT,
                    wDef,
                    0L,
                    aggrOp,
                    KeyedWindowResult::new));

            dag.edge(between(insWm, aggregate)
                    .distributed()
                    .partitioned(entryKey()))
               .edge(between(aggregate, map));
        }

        dag.edge(between(generator, insWm))
           .edge(between(map, writeMap));

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(EXACTLY_ONCE);
        config.setSnapshotIntervalMillis(1200);
        Job job = instance1.getJet().newJob(dag, config);

        JobRepository jobRepository = new JobRepository(instance1);
        int timeout = (int) (MILLISECONDS.toSeconds(config.getSnapshotIntervalMillis() * 3) + 8);

        waitForFirstSnapshot(jobRepository, job.getId(), timeout, false);
        waitForNextSnapshot(jobRepository, job.getId(), timeout, false);
        // wait a little more to emit something, so that it will be overwritten in the sink map
        Thread.sleep(300);

        instance2.getLifecycleService().terminate();

        // Now the job should detect member shutdown and restart from snapshot.
        // Let's wait until the next snapshot appears.
        waitForNextSnapshot(jobRepository, job.getId(),
                (int) (MILLISECONDS.toSeconds(config.getSnapshotIntervalMillis()) + 10), false);
        waitForNextSnapshot(jobRepository, job.getId(), timeout, false);

        job.join();

        // compute expected result
        Map<List<Long>, Long> expectedMap = new HashMap<>();
        for (long partition = 0; partition < numPartitions; partition++) {
            long cnt = 0;
            for (long value = 1; value <= elementsInPartition; value++) {
                cnt++;
                if (value % wDef.frameSize() == 0) {
                    expectedMap.put(asList(value, partition), cnt);
                    cnt = 0;
                }
            }
            if (cnt > 0) {
                expectedMap.put(asList(wDef.higherFrameTs(elementsInPartition - 1), partition), cnt);
            }
        }

        // check expected result
        if (!expectedMap.equals(result)) {
            System.out.println("All expected entries: " + expectedMap.entrySet().stream()
                    .map(Object::toString)
                    .collect(joining(", ")));
            System.out.println("All actual entries: " + result.entrySet().stream()
                    .map(Object::toString)
                    .collect(joining(", ")));
            System.out.println("Non-received expected items: " + expectedMap.keySet().stream()
                    .filter(key -> !result.containsKey(key))
                    .map(Object::toString)
                    .collect(joining(", ")));
            System.out.println("Received non-expected items: " + result.entrySet().stream()
                    .filter(entry -> !expectedMap.containsKey(entry.getKey()))
                    .map(Object::toString)
                    .collect(joining(", ")));
            System.out.println("Different keys: ");
            for (Entry<List<Long>, Long> rEntry : result.entrySet()) {
                Long expectedValue = expectedMap.get(rEntry.getKey());
                if (expectedValue != null && !expectedValue.equals(rEntry.getValue())) {
                    System.out.println("key: " + rEntry.getKey() + ", expected value: " + expectedValue
                            + ", actual value: " + rEntry.getValue());
                }
            }
            System.out.println("-- end of different keys");
            assertEquals(expectedMap, new HashMap<>(result));
        }
    }

    @Test
    public void when_snapshotStartedBeforeExecution_then_firstSnapshotIsSuccessful() {
        // instance1 is always coordinator
        // delay ExecuteOperation so that snapshot is started before execution is started on the worker member
        delayOperationsFrom(instance1, JetInitDataSerializerHook.FACTORY_ID,
                singletonList(JetInitDataSerializerHook.START_EXECUTION_OP)
        );

        DAG dag = new DAG();
        dag.newVertex("p", FirstSnapshotProcessor::new).localParallelism(1);

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(EXACTLY_ONCE);
        config.setSnapshotIntervalMillis(0);
        Job job = instance1.getJet().newJob(dag, config);
        JobRepository repository = new JobRepository(instance1);

        // the first snapshot should succeed
        assertTrueEventually(() -> {
            JobExecutionRecord record = repository.getJobExecutionRecord(job.getId());
            assertNotNull("null JobRecord", record);
            assertEquals(0, record.snapshotId());
        }, 30);
    }

    @Test
    public void when_jobRestartedGracefully_then_noOutputDuplicated() {
        DAG dag = new DAG();
        int elementsInPartition = 100;
        SupplierEx<Processor> sup = () ->
                new SequencesInPartitionsGeneratorP(3, elementsInPartition, true);
        Vertex generator = dag.newVertex("generator", throttle(sup, 30))
                              .localParallelism(1);
        Vertex sink = dag.newVertex("sink", writeListP("sink"));
        dag.edge(between(generator, sink));

        JobConfig config = new JobConfig();
        config.setProcessingGuarantee(EXACTLY_ONCE);
        config.setSnapshotIntervalMillis(3600_000); // set long interval so that the first snapshot does not execute
        Job job = instance1.getJet().newJob(dag, config);

        // wait for the job to start producing output
        List<Entry<Integer, Integer>> sinkList = instance1.getList("sink");
        assertTrueEventually(() -> assertTrue(sinkList.size() > 10));

        // When
        job.restart();
        job.join();

        // Then
        Set<Entry<Integer, Integer>> expected = IntStream.range(0, elementsInPartition)
                 .boxed()
                 .flatMap(i -> IntStream.range(0, 3).mapToObj(p -> entry(p, i)))
                 .collect(Collectors.toSet());
        assertEquals(expected, new HashSet<>(sinkList));
    }

    /**
     * A source, that will generate integer sequences from 0..ELEMENTS_IN_PARTITION,
     * one sequence for each partition.
     * <p>
     * Generated items are {@code entry(partitionId, value)}.
     */
    static class SequencesInPartitionsGeneratorP extends AbstractProcessor {

        private final int numPartitions;
        private final int elementsInPartition;
        private final boolean assertJobRestart;

        private int[] assignedPtions;
        private int[] ptionOffsets;

        private int ptionCursor;
        private MyTraverser traverser;
        private Traverser<Entry<BroadcastKey<Integer>, Integer>> snapshotTraverser;
        private Entry<Integer, Integer> pendingItem;
        private boolean wasRestored;

        SequencesInPartitionsGeneratorP(int numPartitions, int elementsInPartition, boolean assertJobRestart) {
            this.numPartitions = numPartitions;
            this.elementsInPartition = elementsInPartition;
            this.assertJobRestart = assertJobRestart;

            this.traverser = new MyTraverser();
        }

        @Override
        protected void init(@Nonnull Context context) {
            this.assignedPtions = IntStream.range(0, numPartitions)
                                           .filter(i -> i % context.totalParallelism() == context.globalProcessorIndex())
                                           .toArray();
            assert assignedPtions.length > 0 : "no assigned partitions";
            this.ptionOffsets = new int[assignedPtions.length];

            getLogger().info("assignedPtions=" + Arrays.toString(assignedPtions));
        }

        @Override
        public boolean complete() {
            boolean res = emitFromTraverserInt(traverser);
            if (res) {
                assertTrue("Reached end of batch without restoring from a snapshot", wasRestored || !assertJobRestart);
            }
            return res;
        }

        @Override
        public boolean saveToSnapshot() {
            // finish emitting any pending item first before starting snapshot
            if (pendingItem != null) {
                if (tryEmit(pendingItem)) {
                    pendingItem = null;
                } else {
                    return false;
                }
            }
            if (snapshotTraverser == null) {
                snapshotTraverser = Traversers.traverseStream(IntStream.range(0, assignedPtions.length).boxed())
                                              // save {partitionId; partitionOffset} tuples
                                              .map(i -> entry(broadcastKey(assignedPtions[i]), ptionOffsets[i]))
                                              .onFirstNull(() -> snapshotTraverser = null);
                getLogger().info("Saving snapshot, offsets=" + Arrays.toString(ptionOffsets) + ", assignedPtions="
                        + Arrays.toString(assignedPtions));
            }
            return emitFromTraverserToSnapshot(snapshotTraverser);
        }

        @Override
        public void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
            BroadcastKey<Integer> bKey = (BroadcastKey<Integer>) key;
            int partitionIndex = arrayIndexOf(bKey.key(), assignedPtions);
            // restore offset, if assigned to us. Ignore it otherwise
            if (partitionIndex >= 0) {
                ptionOffsets[partitionIndex] = (int) value;
            }
        }

        @Override
        public boolean finishSnapshotRestore() {
            getLogger().info("Restored snapshot, offsets=" + Arrays.toString(ptionOffsets) + ", assignedPtions="
                    + Arrays.toString(assignedPtions));
            // we'll start at the most-behind partition
            advanceCursor();
            wasRestored = true;
            return true;
        }

        // this method is required to keep track of pending item
        private boolean emitFromTraverserInt(MyTraverser traverser) {
            Entry<Integer, Integer> item;
            if (pendingItem != null) {
                item = pendingItem;
                pendingItem = null;
            } else {
                item = traverser.next();
            }
            for (; item != null; item = traverser.next()) {
                if (!tryEmit(item)) {
                    pendingItem = item;
                    return false;
                }
            }
            return true;
        }

        private void advanceCursor() {
            ptionCursor = 0;
            int min = ptionOffsets[0];
            for (int i = 1; i < ptionOffsets.length; i++) {
                if (ptionOffsets[i] < min) {
                    min = ptionOffsets[i];
                    ptionCursor = i;
                }
            }
        }

        private class MyTraverser implements Traverser<Entry<Integer, Integer>> {
            @Override
            public Entry<Integer, Integer> next() {
                try {
                    return ptionOffsets[ptionCursor] < elementsInPartition
                            ? entry(assignedPtions[ptionCursor], ptionOffsets[ptionCursor]) : null;
                } finally {
                    ptionOffsets[ptionCursor]++;
                    advanceCursor();
                }
            }
        }
    }

    /**
     * A source processor which never completes and only allows the first
     * snapshot to finish.
     */
    private static final class FirstSnapshotProcessor extends AbstractProcessor {
        private boolean firstSnapshotDone;

        @Override
        public boolean complete() {
            return false;
        }

        @Override
        public boolean saveToSnapshot() {
            try {
                return !firstSnapshotDone;
            } finally {
                firstSnapshotDone = true;
            }
        }
    }
}
