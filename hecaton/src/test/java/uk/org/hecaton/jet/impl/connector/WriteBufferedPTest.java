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

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.core.DAG;
import uk.org.hecaton.jet.core.Edge;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.TestProcessors;
import uk.org.hecaton.jet.core.TestProcessors.NoOutputSourceP;
import uk.org.hecaton.jet.core.Vertex;
import uk.org.hecaton.jet.core.processor.SinkProcessors;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static uk.org.hecaton.jet.core.test.TestSupport.verifyProcessor;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteBufferedPTest extends JetTestSupport {

    private static final List<String> events = new CopyOnWriteArrayList<>();

    @Before
    public void setup() {
        events.clear();
        TestProcessors.reset(1);
    }

    @Test
    public void writeBuffered_smokeTest() {
        verifyProcessor(getLoggingBufferedWriter())
                .input(asList(1, 2, 3, 4))
                .disableSnapshots()
                .expectOutput(emptyList());

        assertEquals(asList(
                //1st run has inbox limit set to 1 -> it's calling flush after each item
                "new",
                "add:1",
                "flush",
                "add:2",
                "flush",
                "add:3",
                "flush",
                "add:4",
                "flush",
                "dispose",

                //2nd run is with inbox limit set to default
                "new",
                "add:1",
                "add:2",
                "add:3",
                "add:4",
                "flush",
                "dispose"
        ), events);
    }

    @Test
    public void when_writeBufferedJobFailed_then_bufferDisposed() throws Exception {
        HecatonInstance instance = createHecatonInstance();
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", () -> new NoOutputSourceP());
        Vertex sink = dag.newVertex("sink", getLoggingBufferedWriter()).localParallelism(1);

        dag.edge(Edge.between(source, sink));

        Job job = instance.getJet().newJob(dag);
        // wait for the job to initialize
        Thread.sleep(5000);
        job.cancel();

        assertTrueEventually(() -> assertTrue("No \"dispose\", only: " + events, events.contains("dispose")), 60);
        System.out.println(events);
    }

    // returns a processor that will not write anywhere, just log the events
    private static SupplierEx<Processor> getLoggingBufferedWriter() {
        return SinkProcessors.writeBufferedP(
                idx -> {
                    events.add("new");
                    return "foo";
                },
                (buffer, item) -> events.add("add:" + item),
                buffer -> events.add("flush"),
                buffer -> events.add("dispose")
        );
    }
}
