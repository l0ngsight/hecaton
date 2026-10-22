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

package uk.org.hecaton.benchmarks.fanout;

import com.sun.management.HotSpotDiagnosticMXBean;
import jdk.jfr.Recording;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Measures what the members allocate when many clients read the same large value
 * (docs/design/load-protection.md, phase 0): starts a cluster and reader clients in this JVM,
 * stores one value, reads it {@code --gets} times with {@code --concurrency} reads in flight, and
 * records the reads with Flight Recorder. The report says how many value-sized objects the member
 * threads allocated per read and where, and how many bytes each member thread allocated per read.
 * <p>
 * Run it with fixed, small TLABs so that every allocation larger than 64 KB is recorded with its
 * stack ({@code tools/perf/hot-read-allocation.sh} does), one scenario per JVM:
 * <pre>
 * java -XX:TLABSize=64k -XX:-ResizeTLAB -XX:FlightRecorderOptions:stackdepth=96 \
 *      -cp benchmarks/target/benchmarks.jar uk.org.hecaton.benchmarks.fanout.HotReadAllocation \
 *      --product hazelcast --format BINARY --routing ALL_MEMBERS --value-size 1m
 * </pre>
 * Options: {@code --product hecaton|hazelcast}, {@code --caller client|short-lived-client|member},
 * {@code --format BINARY|OBJECT}, {@code --near-cache BINARY|OBJECT} (on the members), {@code --sync}
 * (blocking reads on application threads; with {@code --caller member} they count as member threads),
 * {@code --routing ALL_MEMBERS|SINGLE_MEMBER}, {@code --key-store <pkcs12>} (TLS, Hecaton only),
 * {@code --value-size}, {@code --members}, {@code --readers}, {@code --concurrency}, {@code --gets},
 * {@code --warm-up-gets}, {@code --out <dir>}, {@code --max-copies-per-read <n>} (exit status 1 if
 * exceeded), {@code --load-protection} (Hecaton only: load protection on and the map immutable).
 */
public final class HotReadAllocation {

    /** How long a read refused with an overload exception waits before it is sent again. */
    static final long OVERLOAD_RETRY_MILLIS = 10;

    /** Reads refused with an overload exception (load protection) since the last reset. */
    private static final AtomicLong OVERLOAD_REJECTIONS = new AtomicLong();
    private static final ScheduledExecutorService RETRIES = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "overload-retry");
        thread.setDaemon(true);
        return thread;
    });

    private HotReadAllocation() {
    }

    public static void main(String[] args) throws Exception {
        Scenario scenario;
        try {
            scenario = Scenario.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.err.println("see the javadoc of " + HotReadAllocation.class.getName() + " for the options");
            System.exit(2);
            return;
        }
        quietLogging();
        Files.createDirectories(scenario.out());
        byte[] value = new byte[scenario.valueSize()];
        ThreadLocalRandom.current().nextBytes(value);
        AllocationReport report;
        // blocking reads run on these threads, which outlive the measurement so that their allocations are counted
        AtomicInteger threadIds = new AtomicInteger();
        ExecutorService application = Executors.newFixedThreadPool(scenario.concurrency(),
                task -> new Thread(task, AllocationReport.APPLICATION_THREAD + threadIds.getAndIncrement()));
        try (ReadCluster cluster = ReadCluster.of(scenario.product())) {
            cluster.start(scenario);
            int key = cluster.hotKey();
            cluster.put(key, value);
            read(cluster, scenario, application, key, scenario.warmUpGets());
            System.gc();
            OVERLOAD_REJECTIONS.set(0);

            Path recordingFile = scenario.out().resolve(scenario.name() + ".jfr");
            ThreadAllocations before = ThreadAllocations.snapshot();
            long start = System.nanoTime();
            try (Recording recording = new Recording()) {
                recording.setName(scenario.name());
                recording.enable("jdk.ObjectAllocationInNewTLAB").withStackTrace();
                recording.enable("jdk.ObjectAllocationOutsideTLAB").withStackTrace();
                recording.enable("jdk.GCHeapSummary");
                recording.enable("jdk.GarbageCollection");
                recording.start();
                read(cluster, scenario, application, key, scenario.gets());
                recording.stop();
                recording.dump(recordingFile);
            }
            long elapsedNanos = System.nanoTime() - start;
            ThreadAllocations after = ThreadAllocations.snapshot();
            report = AllocationReport.analyse(scenario, elapsedNanos, before.until(after), recordingFile, fixedTlabs(),
                    OVERLOAD_REJECTIONS.get());
        } finally {
            application.shutdownNow();
        }
        String text = report.render();
        Files.writeString(scenario.out().resolve(scenario.name() + ".md"), text);
        System.out.println(text);
        if (scenario.maxCopies() >= 0 && report.copiesPerRead() > scenario.maxCopies()) {
            System.err.printf("member threads allocated %.2f value-sized objects per read, more than %.2f%n",
                    report.copiesPerRead(), scenario.maxCopies());
            System.exit(1);
        }
        System.exit(0);
    }

    /**
     * Reads {@code gets} times, keeping {@code concurrency} reads in flight over all readers: with
     * {@code getAsync} and a callback, or with {@code --sync} with blocking {@code get}s on as many
     * application threads. A read refused with an overload exception, as load protection refuses
     * reads above its limits, is counted and sent again after {@value #OVERLOAD_RETRY_MILLIS} ms, as
     * an application that backs off would.
     */
    static void read(ReadCluster cluster, Scenario scenario, ExecutorService application, int key, int gets)
            throws Exception {
        if (scenario.sync()) {
            AtomicInteger next = new AtomicInteger();
            List<Future<?>> threads = new ArrayList<>();
            for (int t = 0; t < scenario.concurrency(); t++) {
                threads.add(application.submit(() -> {
                    for (int i = next.getAndIncrement(); i < gets; i = next.getAndIncrement()) {
                        byte[] value = getWithRetries(cluster, i % scenario.readers(), key);
                        if (value == null || value.length != scenario.valueSize()) {
                            throw new IllegalStateException("read a wrong value");
                        }
                    }
                    return null;
                }));
            }
            for (Future<?> thread : threads) {
                thread.get(5, TimeUnit.MINUTES);
            }
            return;
        }
        Semaphore inFlight = new Semaphore(scenario.concurrency());
        CountDownLatch done = new CountDownLatch(gets);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        for (int i = 0; i < gets; i++) {
            if (!inFlight.tryAcquire(1, TimeUnit.MINUTES) || failure.get() != null) {
                break;
            }
            getAsyncWithRetries(cluster, i % scenario.readers(), key).whenComplete((value, error) -> {
                if (error != null) {
                    failure.compareAndSet(null, error);
                } else if (value == null || value.length != scenario.valueSize()) {
                    failure.compareAndSet(null, new IllegalStateException("read a wrong value"));
                }
                inFlight.release();
                done.countDown();
            });
        }
        if (failure.get() != null) {
            throw new IllegalStateException("a read failed", failure.get());
        }
        if (!done.await(5, TimeUnit.MINUTES)) {
            throw new IllegalStateException("reads did not complete in 5 minutes");
        }
        if (failure.get() != null) {
            throw new IllegalStateException("a read failed", failure.get());
        }
    }

    private static byte[] getWithRetries(ReadCluster cluster, int reader, int key) throws InterruptedException {
        while (true) {
            try {
                return cluster.get(reader, key);
            } catch (RuntimeException e) {
                if (!isOverload(e)) {
                    throw e;
                }
                OVERLOAD_REJECTIONS.incrementAndGet();
                Thread.sleep(OVERLOAD_RETRY_MILLIS);
            }
        }
    }

    private static CompletionStage<byte[]> getAsyncWithRetries(ReadCluster cluster, int reader, int key) {
        CompletableFuture<byte[]> result = new CompletableFuture<>();
        getAsyncWithRetries(cluster, reader, key, result);
        return result;
    }

    private static void getAsyncWithRetries(ReadCluster cluster, int reader, int key, CompletableFuture<byte[]> result) {
        cluster.getAsync(reader, key).whenComplete((value, error) -> {
            if (error == null) {
                result.complete(value);
            } else if (isOverload(error)) {
                OVERLOAD_REJECTIONS.incrementAndGet();
                RETRIES.schedule(() -> getAsyncWithRetries(cluster, reader, key, result), OVERLOAD_RETRY_MILLIS,
                        TimeUnit.MILLISECONDS);
            } else {
                result.completeExceptionally(error);
            }
        });
    }

    /** {@code HecatonOverloadException} or Hazelcast's, anywhere in the cause chain. */
    private static boolean isOverload(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t.getClass().getSimpleName().endsWith("OverloadException")) {
                return true;
            }
        }
        return false;
    }

    /** Whether TLABs have a fixed size, so that every large allocation gets its own event. */
    private static boolean fixedTlabs() {
        HotSpotDiagnosticMXBean hotSpot = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
        return "false".equals(hotSpot.getVMOption("ResizeTLAB").getValue());
    }

    private static void quietLogging() {
        Logger root = Logger.getLogger("");
        root.setLevel(Level.WARNING);
        for (Handler handler : root.getHandlers()) {
            handler.setLevel(Level.WARNING);
        }
    }
}
