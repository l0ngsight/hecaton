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
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.core.TestProcessors.MockPS;
import uk.org.hecaton.jet.core.TestProcessors.NoOutputSourceP;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static uk.org.hecaton.jet.core.JobAssertions.assertThat;
import static uk.org.hecaton.jet.core.JobStatus.RUNNING;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category({NightlyTest.class})
public class ScaleUpTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;
    private static final int LOCAL_PARALLELISM = 1;

    private HecatonInstance[] instances;
    private DAG dag;
    private Config config;

    private void setup(long scaleUpDelay) {
        TestProcessors.reset(NODE_COUNT * LOCAL_PARALLELISM);

        dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));
        config = smallInstanceConfig();
        config.getJetConfig().setScaleUpDelayMillis(scaleUpDelay);
        instances = createHecatonInstances(config, NODE_COUNT);
    }

    @Test
    public void when_memberAdded_then_jobScaledUp() {
        setup(1000);
        instances[0].getJet().newJob(dag);
        assertTrueEventually(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()));

        createHecatonInstance(config);
        assertTrueEventually(() -> assertEquals(NODE_COUNT * 2 + 1, MockPS.initCount.get()));
    }

    @Test
    public void when_memberAddedAndAutoScalingDisabled_then_jobNotRestarted() {
        setup(1000);
        instances[0].getJet().newJob(dag, new JobConfig().setAutoScaling(false));
        assertTrueEventually(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()));

        createHecatonInstance(config);
        assertTrueEventually(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()));
    }

    @Test
    public void when_liteMemberAdded_then_jobNotRestarted() {
        setup(1000);
        instances[0].getJet().newJob(dag);
        assertTrueEventually(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()));

        createHecatonInstance(smallInstanceConfig().setLiteMember(true));
        assertTrueEventually(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()));
    }

    @Test
    public void when_memberAddedAndAnotherAddedBeforeDelay_then_jobRestartedOnce() {
        setup(10_000);
        instances[0].getJet().newJob(dag);
        assertTrueEventually(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()));

        createHecatonInstance(config);
        sleepSeconds(1);
        createHecatonInstance(config);
        assertTrueEventually(() -> assertEquals(NODE_COUNT * 2 + 2, MockPS.initCount.get()));
    }

    @Test
    public void when_memberAddedAndRemovedBeforeDelay_then_jobNotRestarted() {
        setup(12_000);
        instances[0].getJet().newJob(dag);
        assertTrueEventually(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()));

        HecatonInstance addedMember = createHecatonInstance(config);
        sleepSeconds(1);
        addedMember.shutdown();
        assertTrueAllTheTime(() -> assertEquals(NODE_COUNT, MockPS.initCount.get()), 15);
    }

    @Test
    public void when_manyJobs() {
        setup(1000);
        List<Job> jobs = new ArrayList<>();
        // we need to disable metrics due to https://github.com/hazelcast/hazelcast/pull/15504
        // TODO remove, after https://github.com/hazelcast/hazelcast/pull/15504 is merged
        JobConfig jobConfig = new JobConfig()
                .setMetricsEnabled(false);
        for (int i = 0; i < Runtime.getRuntime().availableProcessors() * 4; i++) {
            jobs.add(instances[0].getJet().newJob(dag, jobConfig));
        }
        for (Job job : jobs) {
            assertThat(job).eventuallyHasStatus(RUNNING);
        }
        logger.info(jobs.size() + " jobs are running, adding a member");
        createHecatonInstance(config);
        sleepSeconds(2);
        for (Job job : jobs) {
            assertThat(job).eventuallyHasStatus(RUNNING, Duration.ofSeconds(30));
        }
    }
}
