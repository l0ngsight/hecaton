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

package uk.org.hecaton.jet.impl.processor;

import uk.org.hecaton.collection.IList;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.aggregate.AggregateOperation1;
import uk.org.hecaton.jet.core.DAG;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.core.TestProcessors.ListSource;
import uk.org.hecaton.jet.core.Vertex;
import uk.org.hecaton.jet.core.processor.Processors;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static uk.org.hecaton.jet.aggregate.AggregateOperations.summingLong;
import static uk.org.hecaton.jet.core.Edge.between;
import static uk.org.hecaton.jet.core.processor.Processors.combineP;
import static uk.org.hecaton.jet.core.processor.SinkProcessors.writeListP;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Processors_globalAggregationIntegrationTest extends JetTestSupport {

    @Parameter
    public boolean singleStageProcessor;

    @Parameters(name = "singleStageProcessor={0}")
    public static Collection<Object> parameters() {
        return asList(true, false);
    }

    @Test
    public void smokeTest() throws Exception {
        runTest(asList(1L, 2L), 3L);
    }

    private void runTest(List<Long> sourceItems, Long expectedOutput)
            throws Exception {
        HecatonInstance instance = createHecatonInstance();

        AggregateOperation1<Long, ?, Long> summingOp = summingLong((Long l) -> l);

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", () -> new ListSource(sourceItems)).localParallelism(1);
        Vertex sink = dag.newVertex("sink", writeListP("sink"));

        if (singleStageProcessor) {
            Vertex aggregate = dag.newVertex("aggregate", Processors.aggregateP(summingOp))
                    .localParallelism(1);
            dag
                    .edge(between(source, aggregate).distributed().allToOne())
                    .edge(between(aggregate, sink).isolated());

        } else {
            Vertex accumulate = dag.newVertex("accumulate", Processors.accumulateP(summingOp));
            Vertex combine = dag.newVertex("combine", combineP(summingOp)).localParallelism(1);
            dag
                    .edge(between(source, accumulate))
                    .edge(between(accumulate, combine).distributed().allToOne())
                    .edge(between(combine, sink).isolated());
        }

        instance.getJet().newJob(dag).join();

        IList<Long> sinkList = instance.getList("sink");

        assertEquals(singletonList(expectedOutput), new ArrayList<>(sinkList));
        // wait a little more and make sure, that there are no more frames
        Thread.sleep(1000);
        assertEquals(singletonList(expectedOutput), new ArrayList<>(sinkList));

        assertEquals(expectedOutput, sinkList.get(0));
    }
}
