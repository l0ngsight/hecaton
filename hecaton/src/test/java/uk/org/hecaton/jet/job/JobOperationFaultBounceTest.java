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

package uk.org.hecaton.jet.job;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.core.DAG;
import uk.org.hecaton.jet.core.JobStatus;
import uk.org.hecaton.jet.core.TestProcessors;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sinks;
import uk.org.hecaton.jet.pipeline.test.TestSources;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.SlowTest;
import uk.org.hecaton.test.bounce.BounceMemberRule;
import uk.org.hecaton.test.bounce.BounceTestConfiguration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.stream.IntStream;

import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;
import static uk.org.hecaton.test.HecatonTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class JobOperationFaultBounceTest {

    private static final int TEST_DURATION_SECONDS = 30;

    @Rule
    public BounceMemberRule bounceMemberRule =
            BounceMemberRule.with(this::getConfig)
                    .clusterSize(3)
                    .driverCount(1)
                    .driverType(BounceTestConfiguration.DriverType.CLIENT)
                    .avoidOverlappingTerminations(true)
                    .useTerminate(false)
                    .bouncingIntervalSeconds(0)
                    .noSteadyMember()
                    .build();

    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Before
    public void before() {
        var client = bounceMemberRule.getNextTestDriver();

        DAG streamingDag = new DAG();
        streamingDag.newVertex("v", () -> new TestProcessors.MockP().streaming());

        var job = client.getJet().newJob(streamingDag);
        assertTrueEventually(() -> assertEquals("Job should be running", JobStatus.RUNNING, job.getStatus()));
    }

    @Test
    public void submitOperation() {
        var client = bounceMemberRule.getNextTestDriver();
        Runnable[] tasks = new Runnable[] {
                () -> startJob(client, false)
        };
        bounceMemberRule.testRepeatedly(tasks, TEST_DURATION_SECONDS);
    }

    @Test
    public void clientGetJobs() {
        var client = bounceMemberRule.getNextTestDriver();
        Runnable[] tasks = new Runnable[] {
            () -> client.getJet().getJob("job")
        };
        bounceMemberRule.testRepeatedly(tasks, TEST_DURATION_SECONDS);
    }

    @Test
    public void clientGetAllJobs() {
        var client = bounceMemberRule.getNextTestDriver();
        Runnable[] tasks = new Runnable[] {
                () -> {
                    var jobs = client.getJet().getJobs();
                    assertEquals("Expected exactly one job", 1, jobs.size());
                }
        };
        bounceMemberRule.testRepeatedly(tasks, TEST_DURATION_SECONDS);
    }

    private Job startJob(HecatonInstance hz, boolean isLight) {
        Pipeline pipeline = Pipeline.create();
        pipeline.readFrom(TestSources.itemsDistributed(List.of(IntStream.range(0, 5).boxed().toArray(Integer[]::new))))
                .map(e -> e)
                .writeTo(Sinks.noop());
        var jet = hz.getJet();
        return isLight ? jet.newLightJob(pipeline) : jet.newJob(pipeline);
    }
}
