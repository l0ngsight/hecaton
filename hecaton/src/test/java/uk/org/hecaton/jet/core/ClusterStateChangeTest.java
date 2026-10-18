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

import uk.org.hecaton.cluster.Cluster;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.JetService;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.core.TestProcessors.MockPMS;
import uk.org.hecaton.jet.core.TestProcessors.MockPS;
import uk.org.hecaton.jet.core.TestProcessors.NoOutputSourceP;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.cluster.ClusterState.ACTIVE;
import static uk.org.hecaton.cluster.ClusterState.PASSIVE;
import static uk.org.hecaton.jet.core.JobStatus.NOT_RUNNING;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterStateChangeTest extends JetTestSupport {

    private static final int NODE_COUNT = 3;
    private static final int LOCAL_PARALLELISM = 4;
    private static final int TOTAL_PARALLELISM = NODE_COUNT * LOCAL_PARALLELISM;

    private HecatonInstance[] members;
    private HecatonInstance hz;
    private Cluster cluster;
    private DAG dag;

    @Before
    public void before() {
        TestProcessors.reset(TOTAL_PARALLELISM);
        Config config = smallInstanceConfig();
        config.getJetConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);
        members = createHecatonInstances(config, NODE_COUNT);

        assertTrueEventually(() -> {
            for (HecatonInstance instance : members) {
                assertClusterSizeEventually(NODE_COUNT, instance);
            }
        });

        for (HecatonInstance member : members) {
            if (!getNodeEngineImpl(member).getClusterService().isMaster()) {
                hz = member;
                break;
            }
        }

        cluster = hz.getCluster();
        dag = new DAG().vertex(new Vertex("test",
                new MockPMS(() -> new MockPS(NoOutputSourceP::new, NODE_COUNT))));
    }

    @Test
    public void when_clusterPassive_then_jobSubmissionFails() {
        cluster.changeClusterState(PASSIVE);
        assertEquals("Cluster state", PASSIVE, cluster.getClusterState());

        JetService jetService = hz.getJet();
        Assert.assertThrows(IllegalStateException.class, () -> jetService.newJob(dag));
    }

    @Test
    public void when_enterPassiveState_then_executionTerminated() throws Exception {
        // Given
        Job job = hz.getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();

        // When
        cluster.changeClusterState(PASSIVE);

        // Then
        assertEquals("Cluster state", PASSIVE, cluster.getClusterState());
        assertTrue("ProcessorMetaSupplier should get closed", MockPMS.closeCount.get() > 0);
        assertEquals("ProcessorSupplier should get closed on each member", NODE_COUNT, MockPS.closeCount.get());
        assertEquals("Job status", NOT_RUNNING, job.getStatus());

        // Change back to active, so we can stop jobs and check for leaking resources
        cluster.changeClusterState(ACTIVE);
    }

    @Test
    public void when_goPassiveAndBack_then_jobResumes() throws Exception {
        // Given
        hz.getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();

        // When
        cluster.changeClusterState(PASSIVE);
        assertEquals("Cluster state", PASSIVE, cluster.getClusterState());
        TestProcessors.reset(TOTAL_PARALLELISM);
        cluster.changeClusterState(ACTIVE);

        // Then
        assertEquals("Cluster state", ACTIVE, cluster.getClusterState());
        NoOutputSourceP.executionStarted.await();
    }
}
