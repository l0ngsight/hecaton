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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.SimpleTestInClusterSupport;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.impl.exception.JobTerminateRequestedException;
import uk.org.hecaton.jet.impl.exception.TerminatedWithSnapshotException;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sinks;
import uk.org.hecaton.jet.pipeline.test.TestSources;
import uk.org.hecaton.test.ExceptionRecorder;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.logging.Level;

import static uk.org.hecaton.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static uk.org.hecaton.jet.core.JobStatus.FAILED;
import static uk.org.hecaton.jet.core.JobStatus.RUNNING;
import static uk.org.hecaton.jet.core.JobStatus.SUSPENDED;
import static org.assertj.core.api.Assertions.assertThat;

@Category({QuickTest.class, ParallelJVMTest.class})
public class LogExceptionJobTest extends SimpleTestInClusterSupport {

    private static ExceptionRecorder recorder;

    @BeforeClass
    public static void setUpClass() {
        Config config = smallInstanceConfig();
        initialize(2, config);
        recorder = new ExceptionRecorder(instances(), Level.INFO);
    }

    @Before
    public void setUp() throws Exception {
        recorder.clear();
    }

    @Test
    public void when_jobIsSuspended_then_noExceptionIsLogged() {
        // given
        Pipeline pipeline = Pipeline.create()
                                    .readFrom(TestSources.itemStream(10))
                                    .withoutTimestamps()
                                    .writeTo(Sinks.noop())
                                    .getPipeline();
        JobConfig jobConfig = new JobConfig().setProcessingGuarantee(EXACTLY_ONCE);
        Job job = instance().getJet().newJob(pipeline, jobConfig);
        JobAssertions.assertThat(job).eventuallyHasStatus(RUNNING);

        // when
        job.suspend();
        JobAssertions.assertThat(job).eventuallyHasStatus(SUSPENDED);

        // then
        List<Throwable> exceptions = recorder.exceptionsOfTypes(TerminatedWithSnapshotException.class);
        assertThat(exceptions).isEmpty();
    }

    @Test
    public void when_jobIsCancelled_then_noExceptionIsLogged() {
        // given
        Pipeline pipeline = Pipeline.create()
                                    .readFrom(TestSources.itemStream(10))
                                    .withoutTimestamps()
                                    .writeTo(Sinks.noop())
                                    .getPipeline();
        JobConfig jobConfig = new JobConfig().setProcessingGuarantee(EXACTLY_ONCE);
        Job job = instance().getJet().newJob(pipeline, jobConfig);
        JobAssertions.assertThat(job).eventuallyHasStatus(RUNNING);

        // when
        job.cancel();
        JobAssertions.assertThat(job).eventuallyHasStatus(FAILED);

        // then
        List<Throwable> exceptions = recorder.exceptionsOfTypes(JobTerminateRequestedException.class);
        assertThat(exceptions).isEmpty();
    }
}
