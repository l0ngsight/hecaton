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

package uk.org.hecaton.client.impl.spi.impl;

import uk.org.hecaton.internal.metrics.MetricsRegistry;
import uk.org.hecaton.internal.metrics.StaticMetricsProvider;
import uk.org.hecaton.internal.util.executor.LoggingScheduledExecutor;
import uk.org.hecaton.internal.util.executor.PoolExecutorThreadFactory;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.LoggingService;
import uk.org.hecaton.spi.impl.executionservice.TaskScheduler;
import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.spi.properties.HecatonProperty;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.CLIENT_PREFIX_EXECUTION_SERVICE;
import static java.lang.Thread.currentThread;

public final class ClientExecutionServiceImpl implements TaskScheduler, StaticMetricsProvider {

    public static final HecatonProperty INTERNAL_EXECUTOR_POOL_SIZE
            = new HecatonProperty("hecaton.client.internal.executor.pool.size", 3);

    public static final long TERMINATE_TIMEOUT_SECONDS = 30;

    private final ILogger logger;
    private final ScheduledExecutorService internalExecutor;

    public ClientExecutionServiceImpl(String name, ClassLoader classLoader,
                                      HecatonProperties properties, LoggingService loggingService) {
        int internalPoolSize = properties.getInteger(INTERNAL_EXECUTOR_POOL_SIZE);
        if (internalPoolSize <= 0) {
            internalPoolSize = Integer.parseInt(INTERNAL_EXECUTOR_POOL_SIZE.getDefaultValue());
        }
        logger = loggingService.getLogger(TaskScheduler.class);
        internalExecutor = new LoggingScheduledExecutor(logger, internalPoolSize,
                new PoolExecutorThreadFactory(name + ".internal-", classLoader), (r, executor) -> {
            String message = "Internal executor rejected task: " + r + ", because client is shutting down...";
            logger.finest(message);
            throw new RejectedExecutionException(message);
        });
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return internalExecutor.schedule(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<Future<V>> schedule(Callable<V> command, long delay, TimeUnit unit) {
        return (ScheduledFuture<Future<V>>) internalExecutor.schedule(command, delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithRepetition(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return internalExecutor.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public void execute(Runnable command) {
        internalExecutor.execute(command);
    }

    public void shutdown() {
        internalExecutor.shutdown();
        awaitExecutorTermination("internal", internalExecutor, logger);
    }

    public static void awaitExecutorTermination(String name, ExecutorService executor, ILogger logger) {
        try {
            boolean success = executor.awaitTermination(TERMINATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!success) {
                logger.warning(name + " executor awaitTermination could not complete in " + TERMINATE_TIMEOUT_SECONDS
                        + " seconds");
            }
        } catch (InterruptedException e) {
            currentThread().interrupt();
            logger.warning(name + " executor await termination is interrupted", e);
        }
    }

    @Override
    public void provideStaticMetrics(MetricsRegistry registry) {
        registry.registerStaticMetrics(this, CLIENT_PREFIX_EXECUTION_SERVICE);
    }
}
