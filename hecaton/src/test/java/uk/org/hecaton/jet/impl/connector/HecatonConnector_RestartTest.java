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

import uk.org.hecaton.collection.IList;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.impl.JetServiceBackend;
import uk.org.hecaton.jet.impl.JobExecutionService;
import uk.org.hecaton.jet.impl.execution.ExecutionContext;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sinks;
import uk.org.hecaton.jet.pipeline.test.SimpleEvent;
import uk.org.hecaton.jet.pipeline.test.TestSources;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HecatonConnector_RestartTest extends JetTestSupport {

    private HecatonInstance instance1;
    private HecatonInstance instance2;

    @Before
    public void setup() {
        instance1 = createHecatonInstance();
        instance2 = createHecatonInstance();
    }

    @Test
    public void when_iListWrittenAndMemberShutdown_then_jobRestarts() {
        IList<SimpleEvent> sinkList = instance1.getList("list");

        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(10))
                .withoutTimestamps()
                .writeTo(Sinks.list(sinkList));

        Job job = instance1.getJet().newJob(p);
        assertTrueEventually(() -> assertFalse("no output to sink", sinkList.isEmpty()), 10);

        Long executionId = executionId(instance1, job);
        if (executionId == null) {
            executionId = executionId(instance2, job);
        }
        instance2.shutdown();

        // Then - assert that the job stopped producing output
        waitExecutionDoneOnMember(instance1, executionId);
        int sizeAfterShutdown = sinkList.size();

        // Then2 - job restarts and continues production
        assertTrueEventually(() ->
                assertTrue("no output after migration completed", sinkList.size() > sizeAfterShutdown), 20);
    }

    private Long executionId(HecatonInstance instance, Job job) {
        JetServiceBackend jetServiceBackend = getJetServiceBackend(instance);
        JobExecutionService executionService = jetServiceBackend.getJobExecutionService();
        return executionService.getExecutionIdForJobId(job.getId());
    }

    private void waitExecutionDoneOnMember(HecatonInstance instance, long executionId) {
        JetServiceBackend jetServiceBackend = getJetServiceBackend(instance);
        JobExecutionService executionService = jetServiceBackend.getJobExecutionService();
        ExecutionContext execCtx = executionService.getExecutionContext(executionId);
        assertTrueEventually(() -> assertTrue(execCtx == null || execCtx.getExecutionFuture().isDone()));
    }
}
