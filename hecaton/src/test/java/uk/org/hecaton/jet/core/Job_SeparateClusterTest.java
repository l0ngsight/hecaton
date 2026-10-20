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

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientNetworkConfig;
import uk.org.hecaton.client.config.ClusterRoutingConfig;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.JobAlreadyExistsException;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.core.TestProcessors.MockPS;
import uk.org.hecaton.jet.core.TestProcessors.NoOutputSourceP;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static uk.org.hecaton.jet.core.JobAssertions.assertThat;
import static uk.org.hecaton.jet.core.JobStatus.RUNNING;
import static uk.org.hecaton.jet.core.JobStatus.SUSPENDED;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link Job} with a separate cluster for each test.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class Job_SeparateClusterTest extends JetTestSupport {

    private static final int NODE_COUNT = 2;
    private static final int LOCAL_PARALLELISM = 1;

    private HecatonInstance instance1;
    private HecatonInstance instance2;

    @Before
    public void setup() {
        TestProcessors.reset(NODE_COUNT * LOCAL_PARALLELISM);

        Config config = smallInstanceConfig();
        config.getJetConfig().setCooperativeThreadCount(LOCAL_PARALLELISM);
        config.getJetConfig().setScaleUpDelayMillis(10);
        instance1 = createHecatonInstance(config);
        instance2 = createHecatonInstance(config);
    }

    @Test
    public void when_suspendedJobScannedOnNewMaster_then_newJobWithEqualNameFails() {
        // Given
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        JobConfig config = new JobConfig()
                .setName("job1");

        // When
        Job job1 = instance1.getJet().newJob(dag, config);
        assertThat(job1).eventuallyHasStatus(RUNNING);
        job1.suspend();
        assertThat(job1).eventuallyHasStatus(SUSPENDED);
        // gracefully shutdown the master
        instance1.shutdown();

        // Then
        assertThatThrownBy(() -> instance2.getJet().newJob(dag, config))
                .isInstanceOf(JobAlreadyExistsException.class);
    }

    @Test
    public void when_joinFromClientTimesOut_then_futureShouldNotBeCompletedEarly() throws InterruptedException {
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        int timeoutSecs = 1;
        ClientConfig config = new ClientConfig()
                .setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), Integer.toString(timeoutSecs));
        HecatonInstance client = createHecatonClient(config);

        // join request is sent along with job submission
        Job job = client.getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();

        // wait for join invocation to timeout
        Thread.sleep(TimeUnit.SECONDS.toMillis(timeoutSecs));

        // When
        NoOutputSourceP.initCount.set(0);
        instance1.getLifecycleService().terminate();
        // wait for job to be restarted on remaining node
        assertTrueEventually(() -> assertEquals(LOCAL_PARALLELISM, NoOutputSourceP.initCount.get()));

        RuntimeException ex = new RuntimeException("Faulty job");
        NoOutputSourceP.failure.set(ex);

        // Then
        assertThatThrownBy(job::join).hasMessageContaining(ex.getMessage());
    }

    @Test
    public void when_joinFromClientSentToNonMaster_then_futureShouldNotBeCompletedEarly() throws InterruptedException {
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT)));

        int timeoutSecs = 1;
        Address address = Accessors.getAddress(instance2);
        ClientConfig config = new ClientConfig()
                .setProperty(ClientProperty.INVOCATION_TIMEOUT_SECONDS.getName(), Integer.toString(timeoutSecs))
                .setNetworkConfig(new ClientNetworkConfig()
                        .setClusterRoutingConfig(new ClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER))
                        .addAddress(address.getHost() + ":" + address.getPort())
                );
        HecatonInstance client = createHecatonClient(config);

        // join request is sent along with job submission
        Job job = client.getJet().newJob(dag);
        NoOutputSourceP.executionStarted.await();

        // wait for join invocation to timeout
        Thread.sleep(TimeUnit.SECONDS.toMillis(timeoutSecs));

        // When
        NoOutputSourceP.initCount.set(0);
        instance1.getLifecycleService().terminate();
        // wait for job to be restarted on remaining node
        assertTrueEventually(() -> assertEquals(LOCAL_PARALLELISM, NoOutputSourceP.initCount.get()));

        RuntimeException ex = new RuntimeException("Faulty job");
        NoOutputSourceP.failure.set(ex);

        // Then
        assertThatThrownBy(job::join).hasMessageContaining(ex.getMessage());
    }

    @Test
    public void stressTest_getJobStatus_client() throws Exception {
        HecatonInstance client = createHecatonClient();
        stressTest_getJobStatus(() -> client);
    }

    @Test
    @Ignore("fails currently")
    public void stressTest_getJobStatus_member() throws Exception {
        stressTest_getJobStatus(() -> instance1);
    }

    private void stressTest_getJobStatus(Supplier<HecatonInstance> submitterSupplier) throws Exception {
        DAG dag = new DAG().vertex(new Vertex("test", new MockPS(NoOutputSourceP::new, NODE_COUNT * 2)));
        AtomicReference<Job> job = new AtomicReference<>(submitterSupplier.get().getJet().newJob(dag));

        AtomicBoolean done = new AtomicBoolean();
        List<Runnable> actions = asList(
                () -> job.get().getStatus(),
                () -> job.get().getMetrics(),
                () -> job.get().getConfig()
        );
        List<Future<?>> checkerFutures = new ArrayList<>();
        for (Runnable action : actions) {
            checkerFutures.add(spawn(() -> {
                while (!done.get()) {
                    action.run();
                }
            }));
        }

        for (int i = 0; i < 5; i++) {
            instance1.shutdown();
            instance1 = createHecatonInstance();
            job.set(submitterSupplier.get().getJet().getJob(job.get().getId()));
            Job job2 = job.get();
            assertThat(job2).eventuallyHasStatus(RUNNING);

            instance2.shutdown();
            instance2 = createHecatonInstance();
            job.set(submitterSupplier.get().getJet().getJob(job.get().getId()));
            Job job1 = job.get();
            assertThat(job1).eventuallyHasStatus(RUNNING);

            sleepSeconds(1);
            if (checkerFutures.stream().anyMatch(Future::isDone)) {
                break;
            }
        }

        done.set(true);
        for (Future<?> future : checkerFutures) {
            future.get();
        }
    }
}
