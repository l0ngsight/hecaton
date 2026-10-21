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

package uk.org.hecaton.benchmarks;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * IMap get, set and put on a two-member cluster, called on a member or through a client, for
 * Hecaton and Hazelcast CE 5.7 (spec §10: no regression above 5%). Each fork runs one product in
 * its own JVM.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 10)
@Fork(value = 2, jvmArgsAppend = {"-Xms2g", "-Xmx2g", "--enable-native-access=ALL-UNNAMED",
        // the module options both products recommend
        "--add-modules", "java.se", "--add-exports", "java.base/jdk.internal.ref=ALL-UNNAMED",
        "--add-opens", "java.base/java.lang=ALL-UNNAMED", "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens", "java.management/sun.management=ALL-UNNAMED",
        "--add-opens", "jdk.management/com.sun.management.internal=ALL-UNNAMED"})
@Threads(8)
public class MapBenchmark {

    private static final int KEYS = 100_000;

    @Param({"hecaton", "hazelcast"})
    public String product;

    @Param({"member", "client"})
    public String caller;

    @Param({"100", "1000"})
    public int valueSize;

    private Grid grid;
    private byte[] value;

    @Setup(Level.Trial)
    public void setUp() {
        grid = Grid.of(product);
        grid.start(2, "client".equals(caller));
        value = new byte[valueSize];
        ThreadLocalRandom.current().nextBytes(value);
        for (int key = 0; key < KEYS; key++) {
            grid.set(key, value);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        grid.close();
    }

    @Benchmark
    public byte[] get() {
        return grid.get(ThreadLocalRandom.current().nextInt(KEYS));
    }

    @Benchmark
    public void set() {
        grid.set(ThreadLocalRandom.current().nextInt(KEYS), value);
    }

    @Benchmark
    public byte[] put() {
        return grid.put(ThreadLocalRandom.current().nextInt(KEYS), value);
    }
}
