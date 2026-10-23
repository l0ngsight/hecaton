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

import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedStackTrace;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * What the member threads allocated during the measured reads of {@link HotReadAllocation}: exact
 * bytes per thread from {@link ThreadAllocations}, and allocation sites from the Flight Recorder
 * allocation events.
 * <p>
 * An allocation event is recorded for every object that starts a new TLAB or is allocated outside
 * one. With fixed 64 KB TLABs every object larger than 64 KB gets an event, so the count of
 * value-sized objects is exact; smaller objects are sampled, and an event in a new TLAB stands for
 * the TLAB's size (the usual weighting, as in JDK Mission Control).
 */
final class AllocationReport {

    /** Threads of the application that read with {@code --sync}; part of the members' JVM with {@code --caller member}. */
    static final String APPLICATION_THREAD = "application-reader-";

    /** {@code hc.member-0.partition-operation.thread-3}, {@code hz.member-1.IO.thread-out-0}. */
    private static final Pattern MEMBER_THREAD = Pattern.compile("h[cz]\\.(member-\\d+)\\.(.+)");
    private static final int SITE_FRAMES = 8;
    private static final int VALUE_SIZED_SITE_FRAMES = 24;
    private static final int TOP_SITES = 15;
    private static final int TOP_THREADS = 12;

    private final Scenario scenario;
    private final long elapsedNanos;
    private final boolean fixedTlabs;
    private final Map<Row, Long> bytesByThread = new HashMap<>();
    private final Map<Site, Tally> valueSized = new HashMap<>();
    private final Map<Site, Tally> sites = new HashMap<>();
    private final long overloadRejections;
    private long maxHeapAfterGc;
    private long maxHeapBeforeGc;
    private int collections;

    private AllocationReport(Scenario scenario, long elapsedNanos, boolean fixedTlabs, long overloadRejections) {
        this.scenario = scenario;
        this.elapsedNanos = elapsedNanos;
        this.fixedTlabs = fixedTlabs;
        this.overloadRejections = overloadRejections;
    }

    static AllocationReport analyse(Scenario scenario, long elapsedNanos, ThreadAllocations allocations,
                                    Path recordingFile, boolean fixedTlabs, long overloadRejections) throws IOException {
        AllocationReport report = new AllocationReport(scenario, elapsedNanos, fixedTlabs, overloadRejections);
        allocations.bytesByThreadName().forEach((name, bytes) -> {
            Row row = report.row(name);
            if (row != null) {
                report.bytesByThread.merge(row, bytes, Long::sum);
            }
        });
        for (RecordedEvent event : RecordingFile.readAllEvents(recordingFile)) {
            report.add(event);
        }
        return report;
    }

    private void add(RecordedEvent event) {
        switch (event.getEventType().getName()) {
            case "jdk.ObjectAllocationInNewTLAB" -> addAllocation(event, event.getLong("tlabSize"));
            case "jdk.ObjectAllocationOutsideTLAB" -> addAllocation(event, event.getLong("allocationSize"));
            case "jdk.GCHeapSummary" -> {
                if ("After GC".equals(event.getString("when"))) {
                    maxHeapAfterGc = Math.max(maxHeapAfterGc, event.getLong("heapUsed"));
                } else {
                    maxHeapBeforeGc = Math.max(maxHeapBeforeGc, event.getLong("heapUsed"));
                }
            }
            case "jdk.GarbageCollection" -> collections++;
            default -> {
            }
        }
    }

    private void addAllocation(RecordedEvent event, long weight) {
        RecordedThread thread = event.getThread();
        Row row = thread == null ? null : row(thread.getJavaName());
        if (row == null) {
            return;
        }
        long size = event.getLong("allocationSize");
        String type = typeName(event.getClass("objectClass").getName());
        sites.computeIfAbsent(new Site(row, type, site(event.getStackTrace(), SITE_FRAMES)), s -> new Tally()).add(weight);
        if (size >= scenario.valueSize()) {
            valueSized.computeIfAbsent(new Site(row, type, site(event.getStackTrace(), VALUE_SIZED_SITE_FRAMES)),
                    s -> new Tally()).add(size);
        }
    }

    /** Value-sized objects allocated by member threads per read. */
    double copiesPerRead() {
        return valueSized.values().stream().mapToLong(t -> t.count).sum() / (double) scenario.gets();
    }

    String render() {
        StringBuilder out = new StringBuilder();
        double seconds = elapsedNanos / 1e9;
        out.append("## ").append(scenario.name()).append("\n\n");
        out.append("| | |\n|---|---|\n");
        out.append("| Product | ").append(scenario.product().equals("hazelcast") ? "Hazelcast CE 5.7" : "Hecaton").append(" |\n");
        out.append("| Map | `").append(scenario.format()).append("`, hot key owned by `member-0`, ")
                .append(scenario.members()).append(" members")
                .append(scenario.nearCache() == null ? "" : ", near cache `" + scenario.nearCache() + "` on the members")
                .append(" |\n");
        if (scenario.memberCaller()) {
            out.append("| Callers | the map proxies of the members other than `member-0`, TLS ")
                    .append(scenario.tls() ? "on" : "off").append(" |\n");
        } else {
            out.append("| Clients | ").append(scenario.shortLivedClients() ? "a new client for every read"
                            : String.valueOf(scenario.readers())).append(", routing `").append(scenario.routing()).append('`')
                    .append(scenario.routing().equals("SINGLE_MEMBER") ? " (connected to `member-1` only)" : "")
                    .append(", TLS ").append(scenario.tls() ? "on" : "off").append(" |\n");
        }
        out.append(String.format(Locale.ROOT, "| Reads | %,d of a %,d-byte value, %d in flight (%s), %.1f s (%,.0f reads/s) |%n",
                scenario.gets(), scenario.valueSize(), scenario.concurrency(),
                scenario.sync() ? "blocking `get` on as many threads" : "`getAsync` with a callback",
                seconds, scenario.gets() / seconds));
        out.append(String.format(Locale.ROOT, "| Load protection | %s |%n", scenario.loadProtection()
                ? String.format(Locale.ROOT, "on, immutable map; %,d reads refused with an overload exception and retried after"
                        + " %d ms", overloadRejections, HotReadAllocation.OVERLOAD_RETRY_MILLIS) : "off"));
        out.append(String.format(Locale.ROOT, "| JVM heap | at most %s used before a GC and %s after it during the reads, %d"
                + " collections (members and clients share the JVM) |%n", mib(maxHeapBeforeGc), mib(maxHeapAfterGc), collections));
        out.append("| Recording | `").append(scenario.name()).append(".jfr`").append(fixedTlabs ? ""
                : " — **TLABs were not fixed** (`-XX:TLABSize=64k -XX:-ResizeTLAB`): value-sized objects that fit in a"
                + " TLAB are missing below").append(" |\n\n");

        out.append(String.format(Locale.ROOT, "### Value-sized objects allocated by member threads: %.2f per read%n%n",
                copiesPerRead()));
        out.append("Every object of at least the value's size (").append(String.format(Locale.ROOT, "%,d", scenario.valueSize()))
                .append(" bytes) that a member thread allocated, by allocation site. A copy of the value per read shows"
                + " as 1.00.\n\n");
        if (valueSized.isEmpty()) {
            out.append("None.\n\n");
        } else {
            out.append("| Member | Thread | Per read | Object | Size | Allocated at |\n|---|---|---|---|---|---|\n");
            sorted(valueSized).forEach(e -> out.append(String.format(Locale.ROOT, "| %s | %s | %.2f | `%s` | %s | %s |%n",
                    e.getKey().row.member, e.getKey().row.thread, e.getValue().count / (double) scenario.gets(),
                    e.getKey().type, mib(e.getValue().bytes / e.getValue().count), e.getKey().frames)));
            out.append('\n');
        }

        long total = bytesByThread.values().stream().mapToLong(Long::longValue).sum();
        out.append(String.format(Locale.ROOT, "### Bytes allocated by member threads: %,d per read (%.2f × the value)%n%n",
                total / scenario.gets(), total / (double) scenario.gets() / scenario.valueSize()));
        out.append("Exact, from `ThreadMXBean#getThreadAllocatedBytes`, all object sizes. Threads of one pool are added up.\n\n");
        out.append("| Member | Thread | Bytes per read | × value |\n|---|---|---|---|\n");
        bytesByThread.entrySet().stream()
                .sorted(Map.Entry.<Row, Long>comparingByValue().reversed())
                .limit(TOP_THREADS)
                .forEach(e -> out.append(String.format(Locale.ROOT, "| %s | %s | %,d | %.3f |%n", e.getKey().member,
                        e.getKey().thread, e.getValue() / scenario.gets(),
                        e.getValue() / (double) scenario.gets() / scenario.valueSize())));
        out.append('\n');

        out.append("### Largest allocation sites of member threads\n\n");
        out.append("All object sizes, from the allocation events weighted by TLAB size (sampled below 64 KB).\n\n");
        out.append("| Member | Thread | Bytes per read | Object | Allocated at |\n|---|---|---|---|---|\n");
        sorted(sites).limit(TOP_SITES).forEach(e -> out.append(String.format(Locale.ROOT, "| %s | %s | %,d | `%s` | %s |%n",
                e.getKey().row.member, e.getKey().row.thread, e.getValue().bytes / scenario.gets(), e.getKey().type,
                e.getKey().frames)));
        out.append('\n');
        return out.toString();
    }

    private static java.util.stream.Stream<Map.Entry<Site, Tally>> sorted(Map<Site, Tally> tallies) {
        return tallies.entrySet().stream().sorted(Comparator.comparingLong((Map.Entry<Site, Tally> e) -> e.getValue().bytes)
                .reversed());
    }

    private static String site(RecordedStackTrace stackTrace, int depth) {
        if (stackTrace == null) {
            return "(no stack)";
        }
        List<String> frames = new ArrayList<>();
        for (RecordedFrame frame : stackTrace.getFrames()) {
            if (frames.size() == depth) {
                break;
            }
            String type = frame.getMethod().getType().getName();
            frames.add(type.substring(type.lastIndexOf('.') + 1) + "." + frame.getMethod().getName()
                    + ":" + frame.getLineNumber());
        }
        return "`" + String.join("` ← `", frames) + "`";
    }

    private static String typeName(String jvmName) {
        return switch (jvmName) {
            case "[B" -> "byte[]";
            case "[C" -> "char[]";
            case "[I" -> "int[]";
            case "[J" -> "long[]";
            case "[Ljava.lang.Object;" -> "Object[]";
            default -> jvmName.substring(jvmName.lastIndexOf('.') + 1);
        };
    }

    private static String mib(long bytes) {
        return bytes >= 1 << 20 ? String.format(Locale.ROOT, "%.1f MiB", bytes / (double) (1 << 20))
                : String.format(Locale.ROOT, "%,d B", bytes);
    }

    /**
     * The member and thread pool of a member thread, or {@code null} for a thread outside the
     * members. With {@code --caller member} the application's reader threads belong to the members.
     */
    private Row row(String threadName) {
        if (threadName.startsWith(APPLICATION_THREAD)) {
            return scenario.memberCaller() ? new Row("application", "reader") : null;
        }
        Matcher matcher = MEMBER_THREAD.matcher(threadName);
        if (!matcher.matches()) {
            return null;
        }
        String pool = matcher.group(2).replaceAll("\\.thread-\\d+$", "").replaceAll("-\\d+$", "");
        return new Row(matcher.group(1), pool);
    }

    /** A member and one of its threads or thread pools, e.g. {@code member-0} / {@code partition-operation}. */
    private record Row(String member, String thread) {
    }

    private record Site(Row row, String type, String frames) {
    }

    private static final class Tally {
        long count;
        long bytes;

        void add(long size) {
            count++;
            bytes += size;
        }
    }
}
