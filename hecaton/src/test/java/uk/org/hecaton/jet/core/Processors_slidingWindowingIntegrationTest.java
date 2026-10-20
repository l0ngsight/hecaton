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

import uk.org.hecaton.collection.IList;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.function.ToLongFunctionEx;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.Traverser;
import uk.org.hecaton.jet.accumulator.LongAccumulator;
import uk.org.hecaton.jet.aggregate.AggregateOperation;
import uk.org.hecaton.jet.core.processor.Processors;
import uk.org.hecaton.jet.core.processor.SinkProcessors;
import uk.org.hecaton.jet.datamodel.KeyedWindowResult;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static uk.org.hecaton.function.Functions.entryKey;
import static uk.org.hecaton.jet.Traversers.traverseIterable;
import static uk.org.hecaton.jet.aggregate.AggregateOperations.counting;
import static uk.org.hecaton.jet.core.Edge.between;
import static uk.org.hecaton.jet.core.EventTimePolicy.eventTimePolicy;
import static uk.org.hecaton.jet.core.SlidingWindowPolicy.slidingWinPolicy;
import static uk.org.hecaton.jet.core.WatermarkPolicy.limitingLag;
import static uk.org.hecaton.jet.core.processor.Processors.combineToSlidingWindowP;
import static uk.org.hecaton.jet.core.processor.Processors.insertWatermarksP;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HecatonParametrizedRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
public class Processors_slidingWindowingIntegrationTest extends JetTestSupport {

    @Parameter
    public boolean singleStageProcessor;

    @Parameter(1)
    public boolean isBatch;

    @Parameters(name = "singleStageProcessor={0}, isBatch={1}")
    public static Collection<Object> parameters() {
        return asList(
                new Object[]{true, true},
                new Object[]{true, false},
                new Object[]{false, true},
                new Object[]{false, false}
        );
    }

    @Test
    public void smokeTest() throws Exception {
        HecatonInstance instance = createHecatonInstance();

        SlidingWindowPolicy wDef = slidingWinPolicy(2000, 1000);

        DAG dag = new DAG();
        boolean isBatchLocal = isBatch; // to prevent serialization of whole class

        FunctionEx<? super MyEvent, ?> keyFn = MyEvent::getKey;
        ToLongFunctionEx<? super MyEvent> timestampFn = MyEvent::getTimestamp;

        List<MyEvent> inputData = singletonList(new MyEvent(10, "a", 1L));
        Vertex source = dag.newVertex("source", () -> new EmitListP(inputData, isBatchLocal)).localParallelism(1);
        Vertex insertPP = dag.newVertex("insertWmP", insertWatermarksP(eventTimePolicy(
                timestampFn, limitingLag(0), wDef.frameSize(), wDef.frameOffset(), 0
        ))).localParallelism(1);
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP("sink"));

        dag.edge(between(source, insertPP).isolated());

        AggregateOperation<LongAccumulator, Long> counting = counting();
        if (singleStageProcessor) {
            Vertex slidingWin = dag.newVertex("slidingWin",
                    Processors.aggregateToSlidingWindowP(
                            singletonList(keyFn),
                            singletonList(timestampFn),
                            TimestampKind.EVENT,
                            wDef,
                            0L,
                            counting,
                            KeyedWindowResult::new));
            dag
                    .edge(between(insertPP, slidingWin).partitioned(MyEvent::getKey).distributed())
                    .edge(between(slidingWin, sink));

        } else {
            Vertex accumulateByFrame = dag.newVertex("accumulateByFrame",
                    Processors.accumulateByFrameP(
                            singletonList(keyFn),
                            singletonList(timestampFn),
                            TimestampKind.EVENT,
                            wDef,
                            counting.withIdentityFinish()
                    ));
            Vertex slidingWin = dag.newVertex("slidingWin",
                    combineToSlidingWindowP(wDef, counting, KeyedWindowResult::new));
            dag
                    .edge(between(insertPP, accumulateByFrame).partitioned(keyFn))
                    .edge(between(accumulateByFrame, slidingWin).partitioned(entryKey()).distributed())
                    .edge(between(slidingWin, sink));
        }

        Job job = instance.getJet().newJob(dag);

        if (isBatch) {
            job.join();
        }

        IList<MyEvent> sinkList = instance.getList("sink");

        List<KeyedWindowResult<String, Long>> expectedOutput = asList(
                new KeyedWindowResult<>(-1000, 1000, "a", 1L),
                new KeyedWindowResult<>(0, 2000, "a", 1L)
        );
        assertTrueEventually(() -> assertEquals(
                streamToString(expectedOutput.stream()),
                streamToString(new ArrayList<>(sinkList).stream())
        ), 5);
        // wait a little more and make sure, that there are no more frames
        Thread.sleep(1000);
        assertEquals(expectedOutput.size(), sinkList.size());
    }

    private static String streamToString(Stream<?> stream) {
        return stream
                .map(String::valueOf)
                .collect(Collectors.joining("\n"));
    }

    /**
     * A processor that will emit contents of a list and optionally complete.
     */
    private static class EmitListP extends AbstractProcessor {
        private Traverser<Object> traverser;
        private final boolean complete;

        EmitListP(List<?> list, boolean complete) {
            this.traverser = traverseIterable(list);
            this.complete = complete;
            if (!complete) {
                // append a flushing wm for the streaming case
                traverser = traverser.append(new Watermark(100_000));
            }
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            if (!emitFromTraverser(traverser)) {
                return false;
            }
            return complete;
        }
    }

    private static class MyEvent extends SimpleImmutableEntry<String, Long> {
        private final long timestamp;

        MyEvent(long timestamp, String key, Long value) {
            super(key, value);
            this.timestamp = timestamp;
        }

        long getTimestamp() {
            return timestamp;
        }
    }
}
