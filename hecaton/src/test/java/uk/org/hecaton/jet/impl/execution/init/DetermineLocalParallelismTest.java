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

package uk.org.hecaton.jet.impl.execution.init;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.jet.SimpleTestInClusterSupport;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.core.DAG;
import uk.org.hecaton.jet.core.ProcessorMetaSupplier;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Function;

import static uk.org.hecaton.jet.impl.JobExecutionRecord.NO_SNAPSHOT;
import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

@Category({QuickTest.class, ParallelJVMTest.class})
public class DetermineLocalParallelismTest extends SimpleTestInClusterSupport {

    private static final int DEFAULT_PARALLELISM = 2;

    private static NodeEngineImpl nodeEngine;

    @BeforeClass
    public static void before() {
        Config config = new Config();
        config.getJetConfig().setEnabled(true).setCooperativeThreadCount(DEFAULT_PARALLELISM);
        initialize(1, config);
        nodeEngine = getNode(instance()).getNodeEngine();
    }

    @Test
    public void when_preferredLowerThanDefault_then_preferred() {
        testWithParallelism(1, -1, 1);
    }

    @Test
    public void when_preferredGreaterThanDefault_then_default() {
        testWithParallelism(4, -1, DEFAULT_PARALLELISM);
    }

    @Test
    public void when_preferredNotSet_then_default() {
        testWithParallelism(-1, -1, DEFAULT_PARALLELISM);
    }

    @Test
    public void when_vertexSpecifiesParallelism_then_overridesPreferred() {
        testWithParallelism(1, 8, 8);
    }

    @Test
    public void when_vertexSpecifiesParallelism_then_overridesDefault() {
        testWithParallelism(-1, 8, 8);
    }

    private void testWithParallelism(int preferred, int specified, int expected) {
        DAG dag = new DAG();
        dag.newVertex("x", new ValidatingMetaSupplier(preferred, expected))
           .localParallelism(specified);
        validateExecutionPlans(dag);
    }

    private void validateExecutionPlans(DAG dag) {
        ExecutionPlanBuilder.createExecutionPlans(
                nodeEngine,
                ((ClusterServiceImpl) nodeEngine.getClusterService()).getMembershipManager().getMembersView().getMembers(),
                dag, 1, 1, new JobConfig(), NO_SNAPSHOT, false, false, null);
    }

    private static class ValidatingMetaSupplier implements ProcessorMetaSupplier {
        private final int preferredLocalParallelism;
        private final int expectedLocalParallelism;

        ValidatingMetaSupplier(int preferredLocalParallelism, int expectedLocalParallelism) {
            this.preferredLocalParallelism = preferredLocalParallelism;
            this.expectedLocalParallelism = expectedLocalParallelism;
        }

        @Override
        public int preferredLocalParallelism() {
            return preferredLocalParallelism;
        }

        @Override
        public void init(@Nonnull Context context) {
            assertEquals(expectedLocalParallelism, context.localParallelism());
        }

        @Nonnull @Override
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return x -> null;
        }
    }
}
