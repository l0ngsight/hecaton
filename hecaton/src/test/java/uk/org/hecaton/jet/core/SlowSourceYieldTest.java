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

import uk.org.hecaton.jet.SimpleTestInClusterSupport;
import uk.org.hecaton.jet.Traverser;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import java.util.stream.IntStream;

import static uk.org.hecaton.jet.Traversers.traverseStream;
import static uk.org.hecaton.jet.core.Edge.between;
import static uk.org.hecaton.jet.core.processor.Processors.noopP;
import static uk.org.hecaton.jet.impl.util.Util.uncheckRun;
import static org.junit.Assert.assertTrue;

@Category({QuickTest.class, ParallelJVMTest.class})
public class SlowSourceYieldTest extends SimpleTestInClusterSupport {

    @Before
    public void before() {
        initialize(1, null);
    }

    @Test
    public void when_slowSource_then_completeYields() {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", SlowSourceP::new).localParallelism(1);
        Vertex sink = dag.newVertex("sink", noopP()).localParallelism(1);
        dag.edge(between(source, sink));

        instance().getJet().newJob(dag).join();
        assertTrue("processor never yielded", SlowSourceP.yieldCount > 0);
    }

    private static final class SlowSourceP extends AbstractProcessor {
        private static volatile int yieldCount;
        private Traverser<Integer> traverser;
        private int yieldedAt;

        @Override
        protected void init(@Nonnull Context context) {
            // this should take about 5 seconds to emit
            traverser = traverseStream(IntStream.range(0, 5000)
                                                .peek(i -> uncheckRun(() -> Thread.sleep(1)))
                                                .peek(i -> yieldedAt = i)
                                                .boxed());
        }

        @Override
        public boolean complete() {
            boolean res = emitFromTraverser(traverser);
            if (!res) {
                getLogger().info("Yielded at: " + yieldedAt);
                yieldCount++;
            }
            return res;
        }
    }
}
