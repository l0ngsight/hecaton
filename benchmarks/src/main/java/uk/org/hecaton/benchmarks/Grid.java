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

/**
 * A cluster of one product under test, seen through the operations the benchmarks measure.
 */
interface Grid extends AutoCloseable {

    /** Starts {@code members} members on the loopback interface and, if asked, a client. */
    void start(int members, boolean client);

    /** Gets a value of the benchmark map. */
    byte[] get(int key);

    /** Sets a value of the benchmark map. */
    void set(int key, byte[] value);

    /** Puts a value in the benchmark map, returning the previous one. */
    byte[] put(int key, byte[] value);

    @Override
    void close();

    static Grid of(String product) {
        return switch (product) {
            case "hecaton" -> new HecatonGrid();
            case "hazelcast" -> new HazelcastGrid();
            default -> throw new IllegalArgumentException("unknown product " + product);
        };
    }
}
