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

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.core.TestProcessors.DummyStatefulP;
import uk.org.hecaton.jet.core.TestProcessors.MockP;
import uk.org.hecaton.jet.impl.exception.CancellationByUserException;
import uk.org.hecaton.jet.impl.execution.init.JetInitDataSerializerHook;
import uk.org.hecaton.jet.impl.operation.SnapshotPhase1Operation;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.PacketFiltersUtil;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import javax.annotation.Nonnull;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static uk.org.hecaton.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static uk.org.hecaton.test.Accessors.getAddress;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CancellationTest extends JetTestSupport {

    private static final int ASSERTION_TIMEOUT_SECONDS = 15;

    @Parameter
    public boolean useLightJob;

    @Parameters(name = "useLightJob={0}")
    public static Object[] parameters() {
        return new Object[]{true, false};
    }

    @Before
    public void setup() {
        StuckSource.callCounter.set(0);
        FaultyProcessor.failNow = false;
        BlockingProcessor.hasStarted = false;
        BlockingProcessor.isDone = false;
    }

    @After
    public void after() {
        // to not affect other tests in this VM
        SnapshotPhase1Operation.postponeResponses = false;
    }

    @Test
    public void when_jobCancelledOnSingleNode_then_terminatedEventually() {
        // Given
        HecatonInstance instance = createHecatonInstance();

        DAG dag = new DAG();
        dag.newVertex("slow", StuckSource::new);

        Job job = newJob(instance, dag);
        assertExecutionStarted();

        // When
        job.cancel();

        // Then
        assertExecutionTerminated();
        joinAndExpectUserCancellation(job);
    }

    private Job newJob(HecatonInstance instance, DAG dag) {
        return useLightJob ? instance.getJet().newLightJob(dag) : instance.getJet().newJob(dag);
    }

    public static void joinAndExpectUserCancellation(Job job) {
        assertThatThrownBy(job::join).isInstanceOf(CancellationByUserException.class);
    }

    @Test
    public void when_jobCancelledOnMultipleNodes_then_terminatedEventually() {
        // Given
        createHecatonInstance();
        HecatonInstance instance = createHecatonInstance();

        DAG dag = new DAG();
        dag.newVertex("slow", StuckSource::new);

        Job job = newJob(instance, dag);
        assertExecutionStarted();

        // When
        job.cancel();

        // Then
        assertExecutionTerminated();
        joinAndExpectUserCancellation(job);
    }

    @Test
    public void when_jobCancelled_then_jobStatusIsSetEventually() {
        // Given
        HecatonInstance instance = createHecatonInstance();

        DAG dag = new DAG();
        dag.newVertex("slow", StuckSource::new);

        Job job = newJob(instance, dag);
        assertExecutionStarted();

        // When
        job.cancel();

        // Then
        joinAndExpectUserCancellation(job);
        assertThat(job.isUserCancelled()).isTrue();
    }

    @Test
    public void when_jobCancelledFromClient_then_terminatedEventually() {
        // Given
        createHecatonInstance();
        createHecatonInstance();
        HecatonInstance client = createHecatonClient();

        DAG dag = new DAG();
        dag.newVertex("slow", StuckSource::new);

        Job job = newJob(client, dag);
        assertExecutionStarted();

        // When
        job.cancel();

        // Then
        assertExecutionTerminated();
        joinAndExpectUserCancellation(job);
    }

    @Test
    public void when_jobCancelledFromClient_then_jobStatusIsSetEventually() {
        // Given
        createHecatonInstance();
        createHecatonInstance();
        HecatonInstance client = createHecatonClient();

        DAG dag = new DAG();
        dag.newVertex("slow", StuckSource::new);

        Job job = newJob(client, dag);
        assertExecutionStarted();

        // When
        job.cancel();

        // Then
        joinAndExpectUserCancellation(job);
        assertThat(job.isUserCancelled()).isTrue();
    }

    @Test
    public void when_jobCancelled_then_trackedJobsGetNotified() {
        // not applicable to light jobs - they are not tracked after cancellation
        assumeFalse(useLightJob);

        // Given
        HecatonInstance instance1 = createHecatonInstance();
        HecatonInstance instance2 = createHecatonInstance();

        DAG dag = new DAG();
        dag.newVertex("slow", StuckSource::new);

        Job job = newJob(instance1, dag);
        assertExecutionStarted();

        // When
        job.cancel();

        // Then
        assertExecutionTerminated();
        joinAndExpectUserCancellation(job);
        Job tracked = instance2.getJet().getJobs().iterator().next();
        joinAndExpectUserCancellation(tracked);
        assertThat(tracked.isUserCancelled()).isTrue();
    }

    @Test
    public void when_jobFailsOnInitiatorNode_then_cancelledOnOtherNodes() {
        // Given
        HecatonInstance instance = createHecatonInstance();
        createHecatonInstance();

        RuntimeException fault = new RuntimeException("fault");
        DAG dag = new DAG();

        SingleNodeFaultSupplier supplier = new SingleNodeFaultSupplier(getAddress(instance), fault);
        dag.newVertex("faulty", supplier).localParallelism(4);

        Job job = newJob(instance, dag);
        assertExecutionStarted();

        // When
        FaultyProcessor.failNow = true;

        // Then
        assertExecutionTerminated();
        assertThatThrownBy(job::join)
                .isInstanceOf(fault.getClass())
                .hasMessageContaining(fault.getMessage());
        assertThat(job.isUserCancelled()).isFalse();
    }

    @Test
    public void when_jobFailsOnNonInitiatorNode_then_cancelledOnInitiatorNode() {
        // Given
        HecatonInstance instance = createHecatonInstance();
        HecatonInstance other = createHecatonInstance();

        RuntimeException fault = new RuntimeException("fault");
        DAG dag = new DAG();
        dag.newVertex("faulty", new SingleNodeFaultSupplier(getAddress(other), fault))
           .localParallelism(4);

        Job job = newJob(instance, dag);
        assertExecutionStarted();

        // When
        FaultyProcessor.failNow = true;

        // Then
        assertExecutionTerminated();

        assertThatThrownBy(job::join)
                .isInstanceOf(fault.getClass())
                .hasMessageContaining(fault.getMessage());
        assertThat(job.isUserCancelled()).isFalse();
    }

    @Test
    public void when_shutdownGracefully_then_jobFuturesCanceled() {
        when_shutdown_then_jobFuturesCanceled(true);
    }

    @Test
    public void when_shutdownForcefully_then_jobFuturesCanceled() {
        when_shutdown_then_jobFuturesCanceled(false);
    }

    private void when_shutdown_then_jobFuturesCanceled(boolean graceful) {
        HecatonInstance hz = createHecatonInstance();
        DAG dag = new DAG();
        dag.newVertex("blocking", BlockingProcessor::new).localParallelism(1);
        newJob(hz, dag);
        assertTrueEventually(() -> assertTrue(BlockingProcessor.hasStarted), ASSERTION_TIMEOUT_SECONDS);
        if (graceful) {
            hz.shutdown();
        } else {
            hz.getLifecycleService().terminate();
        }
        assertBlockingProcessorEventuallyNotRunning();
    }

    @Test
    public void when_jobCanceled_then_jobFutureCanceled() {
        HecatonInstance hz = createHecatonInstance();
        DAG dag = new DAG();
        dag.newVertex("blocking", BlockingProcessor::new).localParallelism(1);
        Job job = newJob(hz, dag);
        assertTrueEventually(() -> assertTrue(BlockingProcessor.hasStarted), ASSERTION_TIMEOUT_SECONDS);
        job.cancel();
        assertBlockingProcessorEventuallyNotRunning();
    }

    @Test
    public void when_cancellingCompletedJob_then_succeeds() {
        HecatonInstance hz = createHecatonInstance();
        DAG dag = new DAG();
        dag.newVertex("blocking", MockP::new).localParallelism(1);
        Job job = newJob(hz, dag);
        job.join();
        if (!job.isLightJob()) {
            assertEquals(JobStatus.COMPLETED, job.getStatus());
        }

        // When-Then: should not fail
        job.cancel();
        assertThat(job.isUserCancelled()).as("Should not mark completed job as cancelled")
                .isFalse();
    }

    @Test
    public void when_multipleClientsCancel_then_allSucceed() throws Exception {
        HecatonInstance hz = createHecatonInstance();
        DAG dag = new DAG();
        dag.newVertex("blocking", BlockingProcessor::new).localParallelism(1);
        Job job = newJob(hz, dag);
        assertTrueEventually(() -> assertTrue(BlockingProcessor.hasStarted));

        // When-Then: should not fail
        CountDownLatch latch = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            futures.add(spawn(() -> {
                assertOpenEventually(latch);
                job.cancel();
            }));
        }
        latch.countDown();
        for (Future<?> future : futures) {
            future.get();
        }
    }

    @Test
    public void when_cancelledDuringSnapshotPhase1_then_cancelled() {
        assumeFalse(useLightJob);
        HecatonInstance hz = createHecatonInstance();
        SnapshotPhase1Operation.postponeResponses = true;
        DAG dag = new DAG();
        dag.newVertex("blocking", DummyStatefulP::new).localParallelism(1);
        Job job = hz.getJet().newJob(dag, new JobConfig().setSnapshotIntervalMillis(100).setProcessingGuarantee(EXACTLY_ONCE));
        sleepSeconds(2); // wait for the job to start and attempt the 1st snapshot
        cancelAndJoin(job);
        assertThat(job.isUserCancelled()).isTrue();
    }

    @Test
    public void when_cancelledDuringSnapshotPhase2_then_cancelled() {
        assumeFalse(useLightJob);
        HecatonInstance hz = createHecatonInstance();
        createHecatonInstance();
        PacketFiltersUtil.dropOperationsFrom(hz, JetInitDataSerializerHook.FACTORY_ID,
                singletonList(JetInitDataSerializerHook.SNAPSHOT_PHASE2_OPERATION));

        DAG dag = new DAG();
        dag.newVertex("blocking", DummyStatefulP::new).localParallelism(1);
        Job job = hz.getJet().newJob(dag, new JobConfig().setSnapshotIntervalMillis(100).setProcessingGuarantee(EXACTLY_ONCE));
        sleepSeconds(2); // wait for the job to start and attempt the 1st snapshot
        cancelAndJoin(job);
        assertThat(job.isUserCancelled()).isTrue();
    }

    private static void assertExecutionStarted() {
        final long first = StuckSource.callCounter.get();
        assertTrueEventually(() -> assertTrue("Call counter should eventually start being incremented.",
                first != StuckSource.callCounter.get()), ASSERTION_TIMEOUT_SECONDS);
    }

    private static void assertExecutionTerminated() {
        final long[] previous = {0};
        assertTrueEventually(() -> {
            long current = StuckSource.callCounter.get();
            long last = previous[0];
            previous[0] = current;
            assertEquals("Call counter should eventually stop being incremented.", current, last);
            sleepMillis(200);
        }, ASSERTION_TIMEOUT_SECONDS);
    }

    private static void assertBlockingProcessorEventuallyNotRunning() {
        assertTrueEventually(() -> assertTrue(
                String.format("BlockingProcessor should be started and done; hasStarted=%b, isDone=%b",
                        BlockingProcessor.hasStarted, BlockingProcessor.isDone),
                BlockingProcessor.hasStarted && BlockingProcessor.isDone), ASSERTION_TIMEOUT_SECONDS);
    }

    private static class StuckSource extends AbstractProcessor {

        static final AtomicLong callCounter = new AtomicLong();

        @Override
        public boolean complete() {
            callCounter.incrementAndGet();
            sleepMillis(1);
            return false;
        }
    }

    private static class BlockingProcessor extends AbstractProcessor {

        static volatile boolean hasStarted;
        static volatile boolean isDone;

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public boolean complete() {
            hasStarted = true;
            return false;
        }

        @Override
        public void close() {
            isDone = true;
        }
    }

    private static class FaultyProcessor extends AbstractProcessor {

        static volatile boolean failNow;

        private final RuntimeException e;

        FaultyProcessor(@Nonnull RuntimeException e) {
            this.e = e;
        }

        @Override
        public boolean complete() {
            if (failNow) {
                throw e;
            } else {
                return false;
            }
        }
    }

    private static class SingleNodeFaultSupplier implements ProcessorMetaSupplier {

        private final String host;
        private final int port;
        private final RuntimeException e;

        SingleNodeFaultSupplier(Address failOnAddress, RuntimeException e) {
            this.e = e;
            this.host = failOnAddress.getHost();
            this.port = failOnAddress.getPort();
        }

        @Override
        @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            Address failOnAddress;
            try {
                failOnAddress = new Address(host, port);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }

            return address ->
                    ProcessorSupplier.of(address.equals(failOnAddress)
                            ? () -> new FaultyProcessor(e)
                            : StuckSource::new);
        }
    }
}
