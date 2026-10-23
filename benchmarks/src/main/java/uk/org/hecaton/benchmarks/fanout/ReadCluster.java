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

import java.util.concurrent.CompletionStage;

/**
 * Members and reader clients of one product in this JVM, for {@link HotReadAllocation}. Members are
 * named {@code member-0}, {@code member-1}, ... and clients {@code reader-0}, ..., so their threads
 * ({@code hc.member-0.partition-operation.thread-1}, {@code hz.reader-3.internal-1}) tell which
 * side allocated what.
 */
interface ReadCluster extends AutoCloseable {

    /** The map the readers read. */
    String MAP = "fanout";

    /**
     * Starts the members and the clients. The hot key is owned by {@code member-0}; with
     * {@code SINGLE_MEMBER} routing the clients connect to {@code member-1} only, so every read
     * crosses from the member a client is connected to to the key's owner. With
     * {@code --caller member} no clients are started: the members other than {@code member-0} read
     * through their own map proxies, as an application that embeds members does.
     */
    void start(Scenario scenario);

    /** A key whose partition is owned by {@code member-0}. */
    int hotKey();

    /** Stores a value from {@code member-0}. */
    void put(int key, byte[] value);

    /** Reads a value through reader {@code reader}: a client, or a member's map proxy. */
    CompletionStage<byte[]> getAsync(int reader, int key);

    /** Reads a value through reader {@code reader} with a blocking {@code get}. */
    byte[] get(int reader, int key);

    @Override
    void close();

    static ReadCluster of(String product) {
        return switch (product) {
            case "hecaton" -> new HecatonReadCluster();
            case "hazelcast" -> new HazelcastReadCluster();
            default -> throw new IllegalArgumentException("unknown product " + product);
        };
    }
}
