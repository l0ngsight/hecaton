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

package uk.org.hecaton.internal.metrics.impl;

import uk.org.hecaton.config.MetricsConfig;
import uk.org.hecaton.internal.metrics.MetricsPublisher;
import uk.org.hecaton.internal.metrics.MetricsRegistry;
import uk.org.hecaton.internal.metrics.collectors.MetricsCollector;
import uk.org.hecaton.internal.metrics.jmx.JmxPublisher;
import uk.org.hecaton.internal.metrics.managementcenter.ConcurrentArrayRingbuffer;
import uk.org.hecaton.internal.metrics.managementcenter.ConcurrentArrayRingbuffer.RingbufferSlice;
import uk.org.hecaton.internal.metrics.managementcenter.ManagementCenterPublisher;
import uk.org.hecaton.internal.services.ManagedService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.executionservice.ExecutionService;
import uk.org.hecaton.spi.impl.operationservice.LiveOperations;
import uk.org.hecaton.spi.impl.operationservice.LiveOperationsTracker;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static uk.org.hecaton.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static uk.org.hecaton.internal.util.ExceptionUtil.withTryCatch;
import static uk.org.hecaton.internal.util.MapUtil.entry;
import static java.util.stream.Collectors.joining;

/**
 * Service collecting the Metrics periodically and publishes them via
 * {@link MetricsPublisher}s.
 *
 * @since 4.0
 */
public class MetricsService implements ManagedService, LiveOperationsTracker {
    public static final String SERVICE_NAME = "hz:impl:metricsService";

    private final NodeEngine nodeEngine;
    private final ILogger logger;
    private final MetricsConfig config;
    private final LiveOperationRegistry liveOperationRegistry;
    // Holds futures for pending read metrics operations
    private final ConcurrentMap<CompletableFuture<RingbufferSlice<Map.Entry<Long, byte[]>>>, Long>
            pendingReads = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<MetricsPublisher> publishers = new CopyOnWriteArrayList<>();
    private volatile boolean collectorScheduled;

    /**
     * Ringbuffer which stores a bounded history of metrics. For each round of collection,
     * the metrics are compressed into a blob and stored along with the timestamp,
     * with the format (timestamp, byte[])
     */
    private ConcurrentArrayRingbuffer<Map.Entry<Long, byte[]>> metricsJournal;
    private volatile ScheduledFuture<?> scheduledFuture;

    private final Supplier<MetricsRegistry> metricsRegistrySupplier;

    public MetricsService(NodeEngine nodeEngine) {
        this(nodeEngine, ((NodeEngineImpl) nodeEngine)::getMetricsRegistry);
    }

    public MetricsService(NodeEngine nodeEngine, Supplier<MetricsRegistry> metricsRegistrySupplier) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.config = nodeEngine.getConfig().getMetricsConfig();
        this.liveOperationRegistry = new LiveOperationRegistry();
        this.metricsRegistrySupplier = metricsRegistrySupplier;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        if (config.isEnabled()) {

            if (config.getManagementCenterConfig().isEnabled()) {
                publishers.add(createMcPublisher());
            }

            if (config.getJmxConfig().isEnabled()) {
                publishers.add(createJmxPublisher());
            }

            if (!publishers.isEmpty()) {
                scheduleMetricsCollectorIfNeeded();
            }

        } else {
            logger.fine("Metrics collection is disabled");
        }
    }

    /**
     * Register a custom {@link MetricsPublisher} implementation with a register
     * function that takes {@link NodeEngine} as input letting the caller to
     * optionally initialize the publisher returned from the function.
     *
     * @param registerFunction The function that returns with the {@link MetricsPublisher}
     *                         instance.
     */
    public void registerPublisher(Function<NodeEngine, MetricsPublisher> registerFunction) {
        if (config.isEnabled()) {
            MetricsPublisher publisher = registerFunction.apply(nodeEngine);
            publishers.add(publisher);
            scheduleMetricsCollectorIfNeeded();
        } else {
            logger.fine("Custom publisher is not registered with function %s as the metrics system is disabled",
                    registerFunction);
        }
    }

    private void scheduleMetricsCollectorIfNeeded() {
        if (!collectorScheduled && !publishers.isEmpty()) {
            logger.fine("Configuring metrics collection, collection interval=" + config.getCollectionFrequencySeconds()
                + " seconds, retention=" + config.getManagementCenterConfig().getRetentionSeconds() + " seconds, publishers="
                + publishers.stream().map(MetricsPublisher::name).collect(joining(", ", "[", "]")));

            ExecutionService executionService = nodeEngine.getExecutionService();
            scheduledFuture = executionService.scheduleWithRepetition("MetricsPublisher", this::collectMetrics, 1,
                    config.getCollectionFrequencySeconds(), TimeUnit.SECONDS);
            collectorScheduled = true;
        }
    }

    // visible for testing
    MetricsConfig getConfig() {
        return config;
    }

    // visible for testing
    void collectMetrics() {
        MetricsPublisher[] publishersArr = publishers.toArray(new MetricsPublisher[0]);
        PublisherMetricsCollector publisherCollector = new PublisherMetricsCollector(publishersArr);
        collectMetrics(publisherCollector);
        publisherCollector.publishCollectedMetrics();
    }

    // visible for testing
    void collectMetrics(MetricsCollector metricsCollector) {
        metricsRegistrySupplier.get().collect(metricsCollector);
    }

    public LiveOperationRegistry getLiveOperationRegistry() {
        return liveOperationRegistry;
    }

    @Override
    public void populate(LiveOperations liveOperations) {
        liveOperationRegistry.populate(liveOperations);
    }

    /**
     * Read metrics from the journal from the given sequence.
     *
     * @param startSequence The sequence start reading the metrics with.
     */
    public CompletableFuture<RingbufferSlice<Map.Entry<Long, byte[]>>> readMetrics(long startSequence) {
        if (!config.isEnabled()) {
            throw new IllegalArgumentException("Metrics collection is not enabled");
        }
        CompletableFuture<RingbufferSlice<Map.Entry<Long, byte[]>>> future = new CompletableFuture<>();
        future.whenCompleteAsync(withTryCatch(logger, (s, e) -> pendingReads.remove(future)), CALLER_RUNS);
        pendingReads.put(future, startSequence);

        tryCompleteRead(future, startSequence);

        return future;
    }

    private void tryCompleteRead(CompletableFuture<RingbufferSlice<Map.Entry<Long, byte[]>>> future, long sequence) {
        try {
            RingbufferSlice<Map.Entry<Long, byte[]>> slice = metricsJournal.copyFrom(sequence);
            if (!slice.isEmpty()) {
                future.complete(slice);
            }
        } catch (ConcurrentArrayRingbuffer.SequenceOutOfBoundsException e) {
            logger.fine("Error reading from metrics journal, sequence: " + sequence + "."
                    + " In case of persistence-enabled member restart, this error is handled by MC gracefully.", e);
            future.completeExceptionally(e);
        } catch (Exception e) {
            logger.severe("Error reading from metrics journal, sequence: " + sequence, e);
            future.completeExceptionally(e);
        }
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }

        publishers.forEach(MetricsPublisher::shutdown);
    }

    private JmxPublisher createJmxPublisher() {
        return new JmxPublisher(nodeEngine.getHecatonInstance().getName(), "uk.org.hecaton");
    }

    private ManagementCenterPublisher createMcPublisher() {
        int retentionSeconds = config.getManagementCenterConfig().getRetentionSeconds();
        int frequency = config.getCollectionFrequencySeconds();
        int journalSize = Math.max(1, (int) Math.ceil((double) retentionSeconds / frequency));
        metricsJournal = new ConcurrentArrayRingbuffer<>(journalSize);
        return new ManagementCenterPublisher(this.nodeEngine.getLoggingService(),
                (blob, ts) -> {
                    metricsJournal.add(entry(ts, blob));
                    pendingReads.forEach(this::tryCompleteRead);
                }
        );
    }
}
