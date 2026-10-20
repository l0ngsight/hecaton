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

package uk.org.hecaton.jet.core.metrics;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.SimpleTestInClusterSupport;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.config.ProcessingGuarantee;
import uk.org.hecaton.jet.impl.JobRepository;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sinks;
import uk.org.hecaton.jet.pipeline.SourceBuilder;
import uk.org.hecaton.jet.pipeline.StreamSource;
import uk.org.hecaton.jet.pipeline.test.TestSources;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static uk.org.hecaton.jet.core.metrics.MetricNames.EXECUTION_COMPLETION_TIME;
import static uk.org.hecaton.jet.core.metrics.MetricNames.EXECUTION_START_TIME;

@Category({QuickTest.class, ParallelJVMTest.class})
public class JobExecutionMetricsTest extends SimpleTestInClusterSupport {

    private static final long JOB_HAS_NOT_FINISHED_YET_TIME = -1;

    @BeforeClass
    public static void beforeClass() {
        Config config = smallInstanceConfig();
        config.getMetricsConfig().setCollectionFrequencySeconds(1);
        initialize(1, config);
    }

    @Test
    public void testExecutionMetricsBatchJob() {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setStoreMetricsAfterJobCompletion(true);
        Job job = instance().getJet().newJob(batchPipeline(), jobConfig);
        job.join();

        JobMetricsChecker checker = new JobMetricsChecker(job);
        assertTrueEventually(() -> checker.assertSummedMetricValueAtLeast(EXECUTION_START_TIME, 1));

        long executionStartTime = checker.assertSummedMetricValueAtLeast(EXECUTION_START_TIME, 1);
        checker.assertSummedMetricValueAtLeast(EXECUTION_COMPLETION_TIME, executionStartTime);
    }

    @Test
    public void testExecutionMetricsStreamJob() throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setStoreMetricsAfterJobCompletion(true);
        Job job = instance().getJet().newJob(streamPipeline(), jobConfig);

        JobMetricsChecker jobChecker = new JobMetricsChecker(job);
        assertTrueEventually(() -> jobChecker.assertSummedMetricValueAtLeast(EXECUTION_START_TIME, 1));
        JmxMetricsChecker jmxChecker = JmxMetricsChecker.forExecution(instance(), job);

        jmxChecker.assertMetricValueAtLeast(EXECUTION_START_TIME, 1);
        jmxChecker.assertMetricValue(EXECUTION_COMPLETION_TIME, JOB_HAS_NOT_FINISHED_YET_TIME);
    }

    @Test
    public void testExecutionMetricsJobRestart() throws Exception {
        Job job = instance().getJet().newJob(streamPipeline());

        JobMetricsChecker jobChecker = new JobMetricsChecker(job);
        assertTrueEventually(() -> jobChecker.assertSummedMetricValueAtLeast(EXECUTION_START_TIME, 1));
        JmxMetricsChecker jmxChecker = JmxMetricsChecker.forExecution(instance(), job);

        long executionStartTime = jmxChecker.assertMetricValueAtLeast(EXECUTION_START_TIME, 1);
        jmxChecker.assertMetricValue(EXECUTION_COMPLETION_TIME, JOB_HAS_NOT_FINISHED_YET_TIME);

        job.restart();

        jmxChecker.assertMetricValue(EXECUTION_START_TIME, executionStartTime);
        jmxChecker.assertMetricValue(EXECUTION_COMPLETION_TIME, JOB_HAS_NOT_FINISHED_YET_TIME);
    }

    @Test
    public void testExecutionMetricsSuspendResumeWithSnapshot() throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE)
                .setSnapshotIntervalMillis(50);
        Job job = instance().getJet().newJob(snapshotPipeline(), jobConfig);

        JobRepository jr = new JobRepository(instance());
        waitForFirstSnapshot(jr, job.getId(), 20, false);

        JobMetricsChecker jobChecker = new JobMetricsChecker(job);
        assertTrueEventually(() -> jobChecker.assertSummedMetricValueAtLeast(EXECUTION_START_TIME, 1));
        JmxMetricsChecker jmxChecker = JmxMetricsChecker.forExecution(instance(), job);

        long executionStartTime = jmxChecker.assertMetricValueAtLeast(EXECUTION_START_TIME, 1);
        jmxChecker.assertMetricValue(EXECUTION_COMPLETION_TIME, JOB_HAS_NOT_FINISHED_YET_TIME);

        job.restart();

        jmxChecker.assertMetricValue(EXECUTION_START_TIME, executionStartTime);
        jmxChecker.assertMetricValue(EXECUTION_COMPLETION_TIME, JOB_HAS_NOT_FINISHED_YET_TIME);
    }

    private Pipeline streamPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(20))
                .withoutTimestamps()
                .writeTo(Sinks.logger());
        return p;
    }

    private Pipeline batchPipeline() {
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.items(0, 1, 2, 3, 4))
                .writeTo(Sinks.logger());
        return p;
    }

    private Pipeline snapshotPipeline() {
        Pipeline p = Pipeline.create();
        StreamSource<Long> source = SourceBuilder
                .stream("src", procCtx -> new long[1])
                .<Long>fillBufferFn((ctx, buf) -> {
                    buf.add(ctx[0]++);
                    Thread.sleep(5);
                })
                .createSnapshotFn(ctx -> ctx[0])
                .restoreSnapshotFn((ctx, state) -> ctx[0] = state.get(0))
                .build();
        p.readFrom(source)
                .withoutTimestamps()
                .writeTo(Sinks.logger());
        return p;
    }

}
