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
package uk.org.hecaton.persistence.internal.crash;

import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Members are killed ({@code kill -9}) while one of them writes; after a restart of the cluster,
 * every write that was acknowledged before the kill is there (and the write in flight is either
 * there or not).
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class CrashConsistencyTest extends HecatonTestSupport {

    private static final int MIN_ACKS = 3000;
    private static final Pattern ACK = Pattern.compile("ACK (\\d+)");

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final List<Process> processes = new ArrayList<>();
    private final List<HecatonInstance> members = new ArrayList<>();

    @After
    public void tearDown() {
        processes.forEach(Process::destroyForcibly);
        members.forEach(member -> member.getLifecycleService().terminate());
    }

    @Test
    public void singleMemberKilledWhileWriting() throws Exception {
        killWhileWritingAndVerify(1);
    }

    @Test
    public void clusterKilledWhileWriting() throws Exception {
        killWhileWritingAndVerify(3);
    }

    private void killWhileWritingAndVerify(int memberCount) throws Exception {
        File baseDir = folder.newFolder();
        for (int i = 1; i < memberCount; i++) {
            Process idle = start(baseDir, memberCount, "idle");
            awaitReady(idle);
        }
        Process writer = start(baseDir, memberCount, "write");
        AtomicLong lastAck = new AtomicLong(-1);
        CompletableFuture<Void> acks = readAcks(writer, lastAck);
        assertTrueEventually(() -> assertThat(lastAck.get()).isGreaterThanOrEqualTo(MIN_ACKS));
        // kill -9 all members at once, mid-write (through the handle: Process#destroyForcibly also
        // closes the output, and the acknowledgements still in the pipe would be lost)
        processes.forEach(process -> process.toHandle().destroyForcibly());
        for (Process process : processes) {
            assertThat(process.waitFor(30, TimeUnit.SECONDS)).isTrue();
        }
        acks.get(30, TimeUnit.SECONDS);
        long acked = lastAck.get();

        for (int i = 0; i < memberCount; i++) {
            members.add(Hecaton.newHecatonInstance(CrashTestMember.config(baseDir, memberCount)));
        }
        for (HecatonInstance member : members) {
            assertTrueEventually(() -> assertThat(getNode(member).getNodeExtension().isStartCompleted()).isTrue());
        }
        IMap<Integer, Integer> map = members.get(0).getMap(CrashTestMember.MAP);
        Map<Integer, Integer> actual = new HashMap<>();
        for (int key = 0; key < CrashTestMember.KEYS; key++) {
            Integer value = map.get(key);
            if (value != null) {
                actual.put(key, value);
            }
        }
        Map<Integer, Integer> withoutInFlight = apply(acked);
        Map<Integer, Integer> withInFlight = apply(acked + 1);
        assertThat(actual).as("the state after %s acknowledged writes", acked + 1)
                .isIn(withoutInFlight, withInFlight);
    }

    /** The map after the operations up to and including {@code lastSeq}. */
    private static Map<Integer, Integer> apply(long lastSeq) {
        Map<Integer, Integer> map = new HashMap<>();
        for (long seq = 0; seq <= lastSeq; seq++) {
            CrashTestMember.Operation operation = CrashTestMember.operation(seq);
            if (operation.put()) {
                map.put(operation.key(), (int) seq);
            } else {
                map.remove(operation.key());
            }
        }
        return map;
    }

    private Process start(File baseDir, int memberCount, String mode) throws IOException {
        String java = ProcessHandle.current().info().command().orElse("java");
        ProcessBuilder builder = new ProcessBuilder(java, "-Xmx256m", "-Dhecaton.logging.type=none",
                "-cp", System.getProperty("java.class.path"),
                CrashTestMember.class.getName(), baseDir.getAbsolutePath(), Integer.toString(memberCount), mode);
        builder.redirectErrorStream(true);
        Process process = builder.start();
        processes.add(process);
        return process;
    }

    private static void awaitReady(Process process) throws IOException {
        BufferedReader reader = reader(process);
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("READY")) {
                drain(reader);
                return;
            }
        }
        throw new AssertionError("The member process ended before it was ready");
    }

    private static CompletableFuture<Void> readAcks(Process process, AtomicLong lastAck) {
        BufferedReader reader = reader(process);
        return CompletableFuture.runAsync(() -> {
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    Matcher ack = ACK.matcher(line);
                    while (ack.find()) {
                        lastAck.accumulateAndGet(Long.parseLong(ack.group(1)), Math::max);
                    }
                }
            } catch (IOException e) {
                // the process was killed
            }
        });
    }

    private static void drain(BufferedReader reader) {
        CompletableFuture.runAsync(() -> {
            try {
                while (reader.readLine() != null) {
                    // keep the pipe from filling up
                }
            } catch (IOException ignored) {
                // the process was killed
            }
        });
    }

    private static BufferedReader reader(Process process) {
        return new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
    }
}
