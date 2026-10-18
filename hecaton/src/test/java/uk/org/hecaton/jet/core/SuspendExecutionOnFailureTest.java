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

import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.TestInClusterSupport;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.config.ProcessingGuarantee;
import uk.org.hecaton.jet.core.TestProcessors.MockP;
import uk.org.hecaton.jet.core.TestProcessors.MockPS;
import uk.org.hecaton.jet.core.TestProcessors.NoOutputSourceP;
import uk.org.hecaton.jet.core.processor.Processors;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sinks;
import uk.org.hecaton.jet.pipeline.SourceBuilder;
import uk.org.hecaton.jet.pipeline.StreamSource;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

import static uk.org.hecaton.jet.Util.entry;
import static uk.org.hecaton.jet.core.JobStatus.COMPLETED;
import static uk.org.hecaton.jet.core.JobStatus.FAILED;
import static uk.org.hecaton.jet.core.JobStatus.RUNNING;
import static uk.org.hecaton.jet.core.JobStatus.SUSPENDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@Category({QuickTest.class, ParallelJVMTest.class})
public class SuspendExecutionOnFailureTest extends TestInClusterSupport {

    private static final SupplierEx<Throwable> MOCK_ERROR = () -> new AssertionError("mock error");

    private JobConfig jobConfig;

    @Before
    public void before() {
        jobConfig = new JobConfig().setSuspendOnFailure(true);
        TestProcessors.reset(0);
    }

    @Test
    public void when_jobRunning_then_suspensionCauseThrows() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new NoOutputSourceP()));
        Job job = hz().getJet().newJob(dag, jobConfig);
        JobAssertions.assertThat(job).eventuallyHasStatus(RUNNING);

        // Then
        assertThatThrownBy(job::getSuspensionCause)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Job not suspended");

        cancelAndJoin(job);
    }

    @Test
    public void when_jobCompleted_then_suspensionCauseThrows() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", Processors.noopP()));
        Job job = hz().getJet().newJob(dag, jobConfig);

        // When
        job.join();
        assertEquals(COMPLETED, job.getStatus());

        // Then
        assertThatThrownBy(job::getSuspensionCause)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Job not suspended");
    }

    @Test
    public void when_jobFailed_then_suspensionCauseThrows() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", () -> new NoOutputSourceP()));
        Job job = hz().getJet().newJob(dag, jobConfig);
        JobAssertions.assertThat(job).eventuallyHasStatus(RUNNING);

        // When
        job.cancel();
        JobAssertions.assertThat(job).eventuallyHasStatus(FAILED);

        // Then
        assertThatThrownBy(job::getSuspensionCause)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Job not suspended");
    }

    @Test
    public void when_jobSuspendedByUser_then_suspensionCauseSaysSo() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, MEMBER_COUNT)));

        // When
        Job job = hz().getJet().newJob(dag, jobConfig);
        JobAssertions.assertThat(job).eventuallyHasStatus(RUNNING);
        job.suspend();
        JobAssertions.assertThat(job).eventuallySuspended();
        assertThat(job.getSuspensionCause()).matches(JobSuspensionCause::requestedByUser);
        assertThat(job.getSuspensionCause().description()).isEqualTo("Requested by user");
        assertThatThrownBy(job.getSuspensionCause()::errorCause)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Suspension not caused by an error");

        cancelAndJoin(job);
    }

    @Test
    public void when_jobSuspendedDueToFailure_then_suspensionCauseDescribeProblem() {
        // Given
        DAG dag = new DAG();
        dag.newVertex("faulty", () -> new MockP().setCompleteError(MOCK_ERROR));

        // When
        jobConfig.setName("faultyJob");
        Job job = hz().getJet().newJob(dag, jobConfig);

        // Then
        JobAssertions.assertThat(job).eventuallySuspended();

        assertThat(job.getSuspensionCause()).matches(JobSuspensionCause::dueToError);
        assertThat(job.getSuspensionCause().errorCause())
                .isNotNull()
                .matches(error -> error.matches("(?s)Execution failure:\n" +
                        "uk.org.hecaton.jet.JetException: Exception in ProcessorTasklet" +
                        "\\{faultyJob/faulty#[0-9]+}: " +
                        "java.lang.AssertionError: mock error.*"));

        cancelAndJoin(job);
    }

    @Test
    public void when_jobSuspendedDueToFailure_then_canBeResumed() {
        int numItems = 100;
        int interruptItem = 50;

        StreamSource<Integer> source = SourceBuilder.stream("src", procCtx -> new int[1])
                .<Integer>fillBufferFn((ctx, buf) -> {
                    if (ctx[0] < numItems) {
                        buf.add(ctx[0]++);
                        sleepMillis(5);
                    }
                })
                .createSnapshotFn(ctx -> ctx[0])
                .restoreSnapshotFn((ctx, state) -> ctx[0] = state.get(0))
                .build();

        Pipeline p = Pipeline.create();
        p.readFrom(source)
                .withoutTimestamps()
                .<String, Boolean, Map.Entry<Integer, Integer>>mapUsingIMap("SuspendExecutionOnFailureTest_failureMap",
                        item -> "key",
                        (item, value) -> {
                            if (value && item == interruptItem) {
                                throw new RuntimeException("Fail deliberately");
                            }
                            return entry(item, item);
                        }).setLocalParallelism(1)
                .writeTo(Sinks.map("SuspendExecutionOnFailureTest_sinkMap"));

        IMap<String, Boolean> counterMap = hz().getMap("SuspendExecutionOnFailureTest_failureMap");
        counterMap.put("key", true);

        jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE)
                .setSnapshotIntervalMillis(50);

        Job job = hz().getJet().newJob(p, jobConfig);
        JobAssertions.assertThat(job).eventuallyHasStatus(SUSPENDED);

        IMap<Integer, Integer> sinkMap = hz().getMap("SuspendExecutionOnFailureTest_sinkMap");

        counterMap.put("key", false);
        job.resume();
        // sinkMap is an idempotent sink, so even though we're using at-least-once, no key will be duplicated, so it
        // must contain the expected number of items.
        assertTrueEventually(() -> assertEquals(numItems, sinkMap.size()));
        assertTrueEventually(() -> assertEquals(JobStatus.RUNNING, job.getStatus()));

        cancelAndJoin(job);
    }
}
