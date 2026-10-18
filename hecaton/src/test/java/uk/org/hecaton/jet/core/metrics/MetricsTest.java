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
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.jet.Job;
import uk.org.hecaton.jet.accumulator.LongAccumulator;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.core.DAG;
import uk.org.hecaton.jet.core.Edge;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.ResettableSingletonTraverser;
import uk.org.hecaton.jet.core.TestProcessors;
import uk.org.hecaton.jet.core.TestProcessors.NoOutputSourceP;
import uk.org.hecaton.jet.core.Vertex;
import uk.org.hecaton.jet.core.processor.Processors;
import uk.org.hecaton.jet.impl.JobRepository;
import uk.org.hecaton.jet.impl.processor.TransformP;
import uk.org.hecaton.jet.pipeline.Pipeline;
import uk.org.hecaton.jet.pipeline.Sinks;
import uk.org.hecaton.jet.pipeline.test.TestSources;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.function.FunctionEx.identity;
import static uk.org.hecaton.jet.aggregate.AggregateOperations.counting;
import static uk.org.hecaton.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static uk.org.hecaton.jet.core.Edge.between;
import static uk.org.hecaton.jet.core.JobAssertions.assertThat;
import static uk.org.hecaton.jet.core.JobStatus.RUNNING;
import static uk.org.hecaton.jet.core.processor.SinkProcessors.writeListP;
import static uk.org.hecaton.jet.pipeline.ServiceFactories.nonSharedService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MetricsTest extends JetTestSupport {

    private static final JobConfig JOB_CONFIG_WITH_METRICS = new JobConfig().setStoreMetricsAfterJobCompletion(true);

    private final Pipeline pipeline = Pipeline.create();
    private HecatonInstance instance;
    private Config config;

    @Before
    public void before() {
        config = smallInstanceConfig();
        config.setProperty("hecaton.jmx", "true");
        config.getMetricsConfig().setCollectionFrequencySeconds(1);
        instance = createHecatonInstance(config);

        TestProcessors.reset(1);
    }

    @Test
    public void unusedMetrics() {
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filter(l -> {
                    boolean pass = l % 2 == 0;

                    if (!pass) {
                        Metrics.metric("dropped"); //retrieve "dropped" counter, but never use it
                    }
                    //not even retrieve "total" counter

                    return pass;
                })
                .writeTo(Sinks.noop());

        Job job = runPipeline(pipeline.toDag());
        JobMetricsChecker checker = new JobMetricsChecker(job);
        checker.assertSummedMetricValue("dropped", 0);
        checker.assertNoMetricValues("total");
    }

    @Test
    public void typicalUsage() {
        pipeline.readFrom(TestSources.items(5L, 4L, 3L, 2L, 1L, 0L))
                .filter(l -> {
                    boolean pass = l % 2 == 0;

                    if (pass) {
                        Metrics.metric("single-flip-flop").decrement();
                        Metrics.metric("multi-flip-flop").decrement(10);
                    } else {
                        Metrics.metric("dropped").increment();
                        Metrics.metric("single-flip-flop").increment();
                        Metrics.metric("multi-flip-flop").increment(10);
                    }
                    Metrics.metric("total").increment();
                    Metrics.metric("last").set(l);

                    return pass;
                })
                .writeTo(Sinks.noop());

        Job job = runPipeline(pipeline.toDag());
        JobMetricsChecker checker = new JobMetricsChecker(job);
        checker.assertSummedMetricValue("dropped", 3);
        checker.assertSummedMetricValue("total", 6);
        checker.assertSummedMetricValue("single-flip-flop", 0);
        checker.assertSummedMetricValue("multi-flip-flop", 0);
    }

    @Test
    public void customUnit() {
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .mapStateful(LongAccumulator::new, (acc, i) -> {
                    acc.add(i);
                    Metric metric = Metrics.metric("sum", Unit.COUNT);
                    metric.set(acc.get());
                    return acc.get();
                })
                .writeTo(Sinks.noop());

        Job job = runPipeline(pipeline.toDag());
        JobMetricsChecker checker = new JobMetricsChecker(job);
        checker.assertSummedMetricValue("sum", 10);
    }

    @Test
    public void customUnit_notUsed() {
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .mapStateful(LongAccumulator::new, (acc, i) -> {
                    acc.add(i);
                    Metrics.metric("sum", Unit.COUNT);
                    return acc.get();
                })
                .writeTo(Sinks.noop());

        Job job = runPipeline(pipeline.toDag());
        JobMetricsChecker checker = new JobMetricsChecker(job);
        checker.assertSummedMetricValue("sum", 0L);
    }

    @Test
    public void nonCooperativeProcessor() {
        DAG dag = new DAG();

        Vertex source = dag.newVertex("source", TestProcessors.ListSource.supplier(asList(1L, 2L, 3L)));
        Vertex map = dag.newVertex("map", new NonCoopTransformPSupplier((FunctionEx<Long, Long>) l -> {
            Metrics.metric("mapped").increment();
            return l * 10L;
        }));
        Vertex sink = dag.newVertex("sink", writeListP("results"));

        dag.edge(between(source, map)).edge(between(map, sink));

        Job job = runPipeline(dag);
        JobMetricsChecker checker = new JobMetricsChecker(job);
        checker.assertSummedMetricValue("mapped", 3L);
        assertEquals(
                Set.of(10L, 20L, 30L),
                new HashSet<>(instance.getList("results"))
        );
    }

    @Test
    public void metricsDisabled() {
        Long[] input = {0L, 1L, 2L, 3L, 4L};
        pipeline.readFrom(TestSources.items(input))
                .map(l -> {
                    Metrics.metric("mapped").increment();
                    Metrics.metric("total", Unit.COUNT).set(input.length);
                    return l;
                })
                .writeTo(Sinks.noop());

        Job job = instance.getJet().newJob(pipeline, new JobConfig().setMetricsEnabled(false));
        job.join();

        JobMetrics metrics = job.getMetrics();
        assertTrue(metrics.get("mapped").isEmpty());
        assertTrue(metrics.get("total").isEmpty());
    }

    @Test
    public void usingServiceAsync() {
        int inputSize = 100_000;

        Integer[] inputs = new Integer[inputSize];
        Arrays.setAll(inputs, i -> i);

        pipeline.readFrom(TestSources.items(inputs))
                .addTimestamps(i -> i, 0L)
                .mapUsingServiceAsync(
                        nonSharedService(pctx -> 0L),
                        (ctx, l) -> {
                            Metric dropped = Metrics.threadSafeMetric("dropped");
                            Metric total = Metrics.threadSafeMetric("total");
                            return CompletableFuture.supplyAsync(
                                    () -> {
                                        boolean pass = l % 2L == ctx;
                                        if (!pass) {
                                            dropped.increment();
                                        }
                                        total.increment();
                                        return l;
                                    }
                            );
                        }
                )
                .writeTo(Sinks.noop());

        Job job = instance.getJet().newJob(pipeline, JOB_CONFIG_WITH_METRICS);
        assertTrueEventually(() -> assertEquals(inputSize,
                job.getMetrics().get("total").stream().mapToLong(Measurement::value).sum()));
        assertTrueEventually(() -> assertEquals(inputSize / 2,
                job.getMetrics().get("dropped").stream().mapToLong(Measurement::value).sum()));
        job.join();
    }

    @Test
    public void availableDuringJobExecution() {
        int generatedItems = 1000;

        pipeline.readFrom(TestSources.itemStream(1_000))
                .withIngestionTimestamps()
                .filter(l -> l.sequence() < generatedItems)
                .map(t -> {
                    Metrics.metric("total").increment();
                    return t;
                })
                .writeTo(Sinks.noop());

        Job job = instance.getJet().newJob(pipeline, JOB_CONFIG_WITH_METRICS);
        JobMetricsChecker checker = new JobMetricsChecker(job);
        assertTrueEventually(() -> checker.assertSummedMetricValue("total", generatedItems));
    }

    @Test
    public void when_metricsDisabled_then_unavailableDuringJobExecution() {
        int generatedItems = 1000;

        pipeline.readFrom(TestSources.itemStream(1_000))
                .withIngestionTimestamps()
                .filter(l -> l.sequence() < generatedItems)
                .map(t -> {
                    Metrics.metric("total").increment();
                    return t;
                })
                .writeTo(Sinks.list("sink"));

        Job job = instance.getJet().newJob(pipeline, new JobConfig().setMetricsEnabled(false));
        List<Object> list = instance.getList("sink");
        assertTrueEventually(() -> assertFalse(list.isEmpty()));
        assertTrue(job.getMetrics().get("total").isEmpty());
    }

    @Test
    public void when_jetMetricNameIsUsed_then_itIsNotOverwritten() {
        Long[] items = {0L, 1L, 2L, 3L, 4L};
        pipeline.readFrom(TestSources.items(items))
                .filter(l -> {
                    Metrics.metric("emittedCount").increment(1000);
                    return true;
                })
                .writeTo(Sinks.noop());

        Job job = runPipeline(pipeline.toDag());
        new JobMetricsChecker(job, MeasurementPredicates.tagValueEquals("user", "true").negate())
                .assertSummedMetricValue("emittedCount", 10);
        new JobMetricsChecker(job)
                .assertSummedMetricValue("emittedCount", 10 + items.length * 1000);
    }

    @Test
    public void metricInFusedStages() {
        int inputSize = 100_000;
        Integer[] inputs = new Integer[inputSize];
        Arrays.setAll(inputs, i -> i);

        pipeline.readFrom(TestSources.items(inputs))
                .filter(l -> {
                    Metrics.metric("onlyInFilter").increment();
                    Metrics.metric("inBoth").increment();
                    return true;
                })
                .map(t -> {
                    Metrics.metric("onlyInMap").increment();
                    Metrics.metric("inBoth").increment();
                    return t;
                })
                .writeTo(Sinks.noop());

        Job job = runPipeline(pipeline.toDag());
        JobMetricsChecker checker = new JobMetricsChecker(job);
        checker.assertSummedMetricValue("onlyInFilter", inputSize);
        checker.assertSummedMetricValue("onlyInMap", inputSize);
        checker.assertSummedMetricValue("inBoth", 2 * inputSize);
    }

    @Test
    public void metricInNotFusedStages() {
        int inputSize = 100_000;
        Integer[] inputs = new Integer[inputSize];
        Arrays.setAll(inputs, i -> i);

        pipeline.readFrom(TestSources.items(inputs))
                .filter(l -> {
                    Metrics.metric("onlyInFilter").increment();
                    Metrics.metric("inBoth").increment();
                    return true;
                })
                .groupingKey(t -> t)
                .aggregate(counting())
                .map(t -> {
                    Metrics.metric("onlyInMap").increment();
                    Metrics.metric("inBoth").increment();
                    return t;
                })
                .writeTo(Sinks.noop());

        Job job = runPipeline(pipeline.toDag());
        JobMetricsChecker checker = new JobMetricsChecker(job);
        checker.assertSummedMetricValue("onlyInFilter", inputSize);
        checker.assertSummedMetricValue("onlyInMap", inputSize);
        checker.assertSummedMetricValue("inBoth", 2 * inputSize);
    }

    @Test
    public void metricsAreRestartedIfPipelineIsRunTwice() {
        pipeline.readFrom(TestSources.items(0L, 1L, 2L, 3L, 4L))
                .filter(l -> {
                    Metrics.metric("total").increment();
                    return true;
                })
                .writeTo(Sinks.noop());

        Job job = runPipeline(pipeline.toDag());
        new JobMetricsChecker(job).assertSummedMetricValue("total", 5);
        job = runPipeline(pipeline.toDag());
        new JobMetricsChecker(job).assertSummedMetricValue("total", 5);
    }

    @Test
    public void metricsAreJobIndependent() {
        pipeline.readFrom(TestSources.itemStream(1_000))
                .withIngestionTimestamps()
                .filter(l -> l.sequence() < 4000)
                .map(t -> {
                    if (t.sequence() % 4 == 0) {
                        Metrics.metric("total").increment();
                    }
                    return t;
                })
                .writeTo(Sinks.noop());

        Pipeline pipeline2 = Pipeline.create();
        pipeline2.readFrom(TestSources.itemStream(1_000))
                .withIngestionTimestamps()
                .filter(l -> l.sequence() < 4000)
                .map(t -> {
                    if (t.sequence() % 4 != 0) {
                        Metrics.metric("total").increment();
                    }
                    return t;
                })
                .writeTo(Sinks.noop());

        Job job = instance.getJet().newJob(pipeline, JOB_CONFIG_WITH_METRICS);
        Job job2 = instance.getJet().newJob(pipeline2, JOB_CONFIG_WITH_METRICS);
        JobMetricsChecker checker1 = new JobMetricsChecker(job);
        assertTrueEventually(() -> checker1.assertSummedMetricValue("total", 1000));
        JobMetricsChecker checker2 = new JobMetricsChecker(job2);
        assertTrueEventually(() -> checker2.assertSummedMetricValue("total", 3000));
    }

    @Test
    public void availableViaJmx() throws Exception {
        int generatedItems = 1000;

        pipeline.readFrom(TestSources.itemStream(1_000))
                .withIngestionTimestamps()
                .filter(l -> l.sequence() < generatedItems)
                .map(t -> {
                    Metrics.metric("total").increment();
                    return t;
                })
                .writeTo(Sinks.noop());

        Job job = instance.getJet().newJob(pipeline, JOB_CONFIG_WITH_METRICS);
        JobMetricsChecker jobMetricsChecker = new JobMetricsChecker(job);
        assertTrueEventually(() -> jobMetricsChecker.assertSummedMetricValue("total", generatedItems));

        long sum = 0;
        int availableProcessors = config.getJetConfig().getCooperativeThreadCount();
        for (int i = 0; i < availableProcessors; i++) {
            JmxMetricsChecker jmxMetricsChecker = JmxMetricsChecker.forExecution(instance, job,
                    "vertex=fused(filter, map)", "procType=TransformP", "proc=" + i, "user=true");
            long attributeValue = jmxMetricsChecker.getMetricValue("total");
            sum += attributeValue;
        }
        assertEquals(generatedItems, sum);
    }

    @Test
    public void test_sourceSinkTag() {
        DAG dag = new DAG();

        Vertex src = dag.newVertex("src", () -> new NoOutputSourceP());
        Vertex mid = dag.newVertex("mid", Processors.mapP(identity()));
        Vertex sink = dag.newVertex("sink", Processors.noopP());

        dag.edge(Edge.between(src, mid));
        dag.edge(Edge.between(mid, sink));

        Job job = instance.getJet().newJob(dag,
                new JobConfig()
                        .setProcessingGuarantee(EXACTLY_ONCE)
                        .setSnapshotIntervalMillis(100));
        assertThat(job).eventuallyHasStatus(RUNNING);
        JobMetrics metrics = assertJobHasExecutionMetricsEventually(job);

        assertSourceSinkTags(metrics, "src", true, true, false);
        assertSourceSinkTags(metrics, "mid", true, false, false);
        assertSourceSinkTags(metrics, "sink", true, false, true);

        // Restart after a snapshot, so that the job will restart from a snapshot. Check the source/sink tags afterward.
        waitForFirstSnapshot(new JobRepository(instance), job.getId(), 10, true);
        job.restart();

        assertThat(job).eventuallyHasStatus(RUNNING);
        metrics = assertJobHasExecutionMetricsEventually(job);

        assertSourceSinkTags(metrics, "src", false, true, false);
        assertSourceSinkTags(metrics, "mid", false, false, false);
        assertSourceSinkTags(metrics, "sink", false, false, true);
    }

    public static JobMetrics assertJobHasExecutionMetricsEventually(Job job) {
        JobMetrics[] metrics = {null};
        assertTrueEventually(() ->
                assertTrue((metrics[0] = job.getMetrics()).containsTag(MetricTags.EXECUTION)));
        return metrics[0];
    }

    private void assertSourceSinkTags(JobMetrics metrics, String vertexName, boolean beforeRestart,
                                      boolean expectedSource, boolean expectedSink) {
        // get an arbitrary measurement for the vertex, we'll assert the tag only
        Measurement measurement = metrics.filter(MetricTags.VERTEX, vertexName)
                                         .get(MetricNames.EMITTED_COUNT)
                                         .get(0);

        assertEquals("vertex=" + vertexName + ", metric=" + MetricTags.SOURCE + ", beforeRestart=" + beforeRestart,
                expectedSource ? "true" : null, measurement.tag(MetricTags.SOURCE));
        assertEquals("vertex=" + vertexName + ", metric=" + MetricTags.SINK + ", beforeRestart=" + beforeRestart,
                expectedSink ? "true" : null, measurement.tag(MetricTags.SINK));
    }

    private Job runPipeline(DAG dag) {
        Job job = instance.getJet().newJob(dag, JOB_CONFIG_WITH_METRICS);
        job.join();
        return job;
    }

    private static class NonCoopTransformPSupplier implements SupplierEx<Processor> {

        private final FunctionEx<Long, Long> mappingFn;

        NonCoopTransformPSupplier(FunctionEx<Long, Long> mappingFn) {
            this.mappingFn = mappingFn;
        }

        @Override
        public Processor getEx() {
            final ResettableSingletonTraverser<Long> trav = new ResettableSingletonTraverser<>();
            return new TransformP<Long, Long>(item -> {
                trav.accept(((FunctionEx<? super Long, ? extends Long>) mappingFn).apply(item));
                return trav;
            }) {
                @Override
                public boolean isCooperative() {
                    return false;
                }
            };
        }
    }

}
