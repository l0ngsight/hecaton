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

import uk.org.hecaton.internal.cluster.MemberInfo;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.jet.SimpleTestInClusterSupport;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.core.AbstractProcessor;
import uk.org.hecaton.jet.core.DAG;
import uk.org.hecaton.jet.core.Edge;
import uk.org.hecaton.jet.core.ProcessorMetaSupplier;
import uk.org.hecaton.jet.core.Vertex;
import uk.org.hecaton.jet.impl.JetServiceBackend;
import uk.org.hecaton.jet.impl.JobClassLoaderService.JobPhase;
import uk.org.hecaton.jet.impl.MasterJobContext;
import uk.org.hecaton.jet.impl.execution.SnapshotContext;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static uk.org.hecaton.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static uk.org.hecaton.jet.core.Edge.between;
import static uk.org.hecaton.jet.core.Edge.from;
import static uk.org.hecaton.jet.impl.execution.init.ExecutionPlanBuilder.createExecutionPlans;
import static uk.org.hecaton.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@Category({QuickTest.class, ParallelJVMTest.class})
public class VertexDef_HigherPrioritySourceTest extends SimpleTestInClusterSupport {

    private static final ProcessorMetaSupplier MOCK_PMS =
            addresses -> address -> count -> nCopies(count, new DummyProcessor());
    private static NodeEngineImpl nodeEngineImpl;
    private static List<MemberInfo> membersView;

    private DAG dag = new DAG();
    private Vertex v1 = dag.newVertex("v1", MOCK_PMS);
    private Vertex v2 = dag.newVertex("v2", MOCK_PMS);
    private Vertex v3 = dag.newVertex("v3", MOCK_PMS);
    private Vertex v4 = dag.newVertex("v4", MOCK_PMS);
    private Vertex v5 = dag.newVertex("v5", MOCK_PMS);

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
        nodeEngineImpl = Accessors.getNodeEngineImpl(instance());
        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngineImpl.getClusterService();
        membersView = clusterService.getMembershipManager().getMembersView().getMembers();
    }

    @Test
    public void test_simple() {
        dag.edge(from(v1, 0).to(v3, 0).priority(-1))
           .edge(from(v2, 0).to(v3, 1));

        assertHigherPriorityVertices(v1);
    }

    @Test
    public void test_nested() {
        dag.edge(between(v1, v2))
           .edge(from(v2, 0).to(v4, 0).priority(-1))
           .edge(from(v3, 0).to(v4, 1));

        assertHigherPriorityVertices(v1, v2);
    }

    @Test
    public void test_nestedMore() {
        dag.edge(from(v1, 0).to(v3, 0))
           .edge(from(v2, 0).to(v3, 1))
           .edge(from(v3, 0).to(v5, 0).priority(-1))
           .edge(from(v4, 0).to(v5, 1));

        assertHigherPriorityVertices(v1, v2, v3);
    }

    @Test
    public void test_noHigherPrioritySource() {
        assertHigherPriorityVertices();
    }

    @Test
    public void test_noHigherPrioritySource_2() {
        dag.edge(between(v1, v2));
        assertHigherPriorityVertices();
    }

    @Test
    public void test_snapshotRestoreEdge() throws Exception {
        Edge edge = between(v1, v2);
        forceSnapshotPriority(edge);
        dag.edge(edge);
        assertHigherPriorityVertices(v1);
    }

    private void forceSnapshotPriority(Edge edge) throws Exception {
        Field field = Edge.class.getDeclaredField("priority");
        field.setAccessible(true);
        field.set(edge, MasterJobContext.SNAPSHOT_RESTORE_EDGE_PRIORITY);
    }

    private void assertHigherPriorityVertices(Vertex... vertices) {
        JobConfig jobConfig = new JobConfig();
        Map<MemberInfo, ExecutionPlan> executionPlans;
        try {
            executionPlans = createExecutionPlans(nodeEngineImpl, membersView, dag, 0, 0, jobConfig, 0, false, false, null).get();
        } catch (InterruptedException | ExecutionException e) {
            throw rethrow(e);
        }
        ExecutionPlan plan = executionPlans.values().iterator().next();
        SnapshotContext ssContext = new SnapshotContext(mock(ILogger.class), "job", 0, EXACTLY_ONCE);

        // In the production code the plan#initialize is only called from places where we have already set up the
        // processor classloaders
        JetServiceBackend jetService = nodeEngineImpl.getService(JetServiceBackend.SERVICE_NAME);
        jetService.getJobClassLoaderService().getOrCreateClassLoader(jobConfig, 0, JobPhase.EXECUTION);
        try {
            jetService.getJobClassLoaderService().prepareProcessorClassLoaders(0);
            plan.initialize(nodeEngineImpl, 0, 0, ssContext, null,
                    (InternalSerializationService) nodeEngineImpl.getSerializationService()).join();
        } finally {
            jetService.getJobClassLoaderService().clearProcessorClassLoaders();
        }
        jetService.getJobClassLoaderService().tryRemoveClassloadersForJob(0, JobPhase.EXECUTION);

        Set<Integer> higherPriorityVertices = VertexDef.getHigherPriorityVertices(plan.getVertices().toArray(new VertexDef[0]));
        String actualHigherPriorityVertices = plan.getVertices().stream()
                .filter(v -> higherPriorityVertices.contains(v.vertexId()))
                .map(VertexDef::name)
                .sorted()
                .collect(joining("\n"));
        String expectedVertices = Arrays.stream(vertices).map(Vertex::getName).sorted().collect(joining("\n"));
        assertEquals(expectedVertices, actualHigherPriorityVertices);
    }

    private static class DummyProcessor extends AbstractProcessor {
    }
}
