/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.internal.loadprotection;

import uk.org.hecaton.config.LoadProtectionConfig;
import uk.org.hecaton.internal.metrics.Probe;
import uk.org.hecaton.internal.util.ThreadUtil;
import uk.org.hecaton.internal.util.counters.MwCounter;
import uk.org.hecaton.logging.ILogger;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static uk.org.hecaton.internal.metrics.ProbeLevel.MANDATORY;
import static uk.org.hecaton.internal.util.counters.MwCounter.newMwCounter;

/**
 * The threads and limits for client connection lifecycle work (load protection, F23), so that
 * connection churn does not delay map and queue operations:
 * <ul>
 *     <li>the cleanup after client disconnects, client authentication and the client monitors run on
 *     the lane's own threads instead of the event, generic operation and scheduled threads;</li>
 *     <li>the parked operations of disconnected clients are cancelled in batches, one pass over the
 *     wait sets per batch instead of per client;</li>
 *     <li>new client connections are refused while the accept rate, the authentications in
 *     progress or the lane's backlog are above their limits.</li>
 * </ul>
 * Cleanup is never dropped: when the backlog is full, it runs on the caller's thread.
 */
public final class MaintenanceLane {

    /** Disconnected clients are cancelled in batches of at most this many. */
    static final int DISCONNECT_BATCH = 1_000;
    private static final long FLUSH_MILLIS = 100;
    private static final long SUMMARY_MILLIS = TimeUnit.MINUTES.toMillis(1);

    @Probe(name = "maintenanceBacklog", level = MANDATORY)
    private final AtomicInteger backlog = new AtomicInteger();
    @Probe(name = "maintenanceTasks", level = MANDATORY)
    private final MwCounter tasks = newMwCounter();
    @Probe(name = "maintenanceTasksRunInline", level = MANDATORY)
    private final MwCounter tasksRunInline = newMwCounter();
    @Probe(name = "disconnectBatches", level = MANDATORY)
    private final MwCounter disconnectBatches = newMwCounter();
    @Probe(name = "disconnectedClients", level = MANDATORY)
    private final MwCounter disconnectedClients = newMwCounter();
    @Probe(name = "authenticationsInProgress", level = MANDATORY)
    private final AtomicInteger authentications = new AtomicInteger();
    @Probe(name = "clientConnectionsAccepted", level = MANDATORY)
    private final MwCounter accepted = newMwCounter();
    @Probe(name = "clientConnectionsRefused", level = MANDATORY)
    private final MwCounter refused = newMwCounter();

    private final ThreadPoolExecutor executor;
    private final int maxBacklog;
    private final int maxConcurrentAuthentications;
    private final RateLimiter acceptRate;
    private final ILogger logger;
    private final Queue<UUID> disconnected = new ConcurrentLinkedQueue<>();
    private final AtomicInteger pendingDisconnects = new AtomicInteger();
    private final AtomicBoolean flushScheduled = new AtomicBoolean();
    private volatile Consumer<Set<UUID>> disconnectHandler;
    private long acceptedAtLastSummary;
    private long refusedAtLastSummary;
    private long disconnectedAtLastSummary;

    MaintenanceLane(String instanceName, LoadProtectionConfig.MaintenanceConfig config, ScheduledExecutorService scheduler,
                    ILogger logger) {
        this.maxBacklog = config.getMaxBacklog();
        this.maxConcurrentAuthentications = config.getMaxConcurrentAuthentications();
        this.acceptRate = config.getMaxAcceptsPerSecond() > 0 ? new RateLimiter(config.getMaxAcceptsPerSecond()) : null;
        this.logger = logger;
        String threadName = ThreadUtil.createThreadName(instanceName, "client-maintenance");
        AtomicInteger threadIds = new AtomicInteger();
        this.executor = new ThreadPoolExecutor(config.getThreadCount(), config.getThreadCount(), 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(maxBacklog), task -> {
                    Thread thread = new Thread(task, threadName + "-" + threadIds.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                });
        scheduler.scheduleWithFixedDelay(this::scheduleFlush, FLUSH_MILLIS, FLUSH_MILLIS, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::logSummary, SUMMARY_MILLIS, SUMMARY_MILLIS, TimeUnit.MILLISECONDS);
    }

    /** Sets who cancels the parked operations of disconnected clients, given a batch of their UUIDs. */
    public void setDisconnectHandler(Consumer<Set<UUID>> disconnectHandler) {
        this.disconnectHandler = disconnectHandler;
    }

    /** Runs connection lifecycle work on the lane, or on this thread if the lane's backlog is full. */
    public void execute(Runnable task) {
        tasks.inc();
        backlog.incrementAndGet();
        try {
            executor.execute(() -> {
                backlog.decrementAndGet();
                runLogged(task);
            });
        } catch (RejectedExecutionException e) {
            backlog.decrementAndGet();
            tasksRunInline.inc();
            runLogged(task);
        }
    }

    /** Runs {@code task} on the lane at a fixed rate, for the client monitors. */
    public void scheduleWithFixedDelay(ScheduledExecutorService scheduler, Runnable task, long delay, TimeUnit unit) {
        AtomicBoolean queued = new AtomicBoolean();
        scheduler.scheduleWithFixedDelay(() -> {
            // at most one run of the task waits on the lane
            if (queued.compareAndSet(false, true)) {
                execute(() -> {
                    queued.set(false);
                    task.run();
                });
            }
        }, delay, delay, unit);
    }

    /** Collects a disconnected client; its parked operations are cancelled with the next batch. */
    public void clientDisconnected(UUID clientUuid) {
        disconnected.add(clientUuid);
        disconnectedClients.inc();
        if (pendingDisconnects.incrementAndGet() >= DISCONNECT_BATCH) {
            scheduleFlush();
        }
    }

    /**
     * Whether to accept a new client connection: refused while the accept rate or the lane's
     * backlog are above their limits.
     */
    public boolean admitNewConnection() {
        boolean admit = backlog.get() < maxBacklog && (acceptRate == null || acceptRate.tryAcquire());
        if (admit) {
            accepted.inc();
        } else {
            refused.inc();
        }
        return admit;
    }

    /** Whether another client authentication may start; if so, {@link #authenticationDone()} must follow. */
    public boolean tryStartAuthentication() {
        if (authentications.incrementAndGet() > maxConcurrentAuthentications) {
            authentications.decrementAndGet();
            refused.inc();
            return false;
        }
        return true;
    }

    public void authenticationDone() {
        authentications.decrementAndGet();
    }

    public int backlog() {
        return backlog.get();
    }

    public long disconnectBatches() {
        return disconnectBatches.get();
    }

    public long refusedConnections() {
        return refused.get();
    }

    void shutdown() {
        executor.shutdownNow();
    }

    private void scheduleFlush() {
        if (pendingDisconnects.get() > 0 && flushScheduled.compareAndSet(false, true)) {
            execute(this::flushDisconnects);
        }
    }

    private void flushDisconnects() {
        flushScheduled.set(false);
        Consumer<Set<UUID>> handler = disconnectHandler;
        while (pendingDisconnects.get() > 0) {
            Set<UUID> batch = new HashSet<>();
            UUID uuid;
            while (batch.size() < DISCONNECT_BATCH && (uuid = disconnected.poll()) != null) {
                batch.add(uuid);
                pendingDisconnects.decrementAndGet();
            }
            if (batch.isEmpty()) {
                return;
            }
            disconnectBatches.inc();
            if (handler != null) {
                handler.accept(batch);
            }
        }
    }

    private void logSummary() {
        long acceptedNow = accepted.get();
        long refusedNow = refused.get();
        long disconnectedNow = disconnectedClients.get();
        if (acceptedNow != acceptedAtLastSummary || refusedNow != refusedAtLastSummary
                || disconnectedNow != disconnectedAtLastSummary) {
            logger.info("Client connections in the last minute: " + (acceptedNow - acceptedAtLastSummary) + " accepted, "
                    + (refusedNow - refusedAtLastSummary) + " refused, " + (disconnectedNow - disconnectedAtLastSummary)
                    + " disconnected; maintenance backlog " + backlog.get());
            acceptedAtLastSummary = acceptedNow;
            refusedAtLastSummary = refusedNow;
            disconnectedAtLastSummary = disconnectedNow;
        }
    }

    private void runLogged(Runnable task) {
        try {
            task.run();
        } catch (Throwable t) {
            logger.warning("Client maintenance task failed", t);
        }
    }

    /** At most {@code permitsPerSecond} in each one-second window. */
    private static final class RateLimiter {
        private final int permitsPerSecond;
        private long windowStart = System.nanoTime();
        private int used;

        RateLimiter(int permitsPerSecond) {
            this.permitsPerSecond = permitsPerSecond;
        }

        synchronized boolean tryAcquire() {
            long now = System.nanoTime();
            if (now - windowStart >= TimeUnit.SECONDS.toNanos(1)) {
                windowStart = now;
                used = 0;
            }
            if (used < permitsPerSecond) {
                used++;
                return true;
            }
            return false;
        }
    }
}
