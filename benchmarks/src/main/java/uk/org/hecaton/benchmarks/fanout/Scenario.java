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

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * One measured configuration of {@link HotReadAllocation}.
 *
 * @param product     {@code hecaton} or {@code hazelcast} (Hazelcast CE 5.7)
 * @param format      the map's in-memory format: {@code BINARY} or {@code OBJECT}
 * @param nearCache   in-memory format of a near cache on the members' map proxies ({@code BINARY} or
 *                    {@code OBJECT}), or {@code null} for none; it serves {@code --caller member} reads
 * @param caller      who reads: {@code client} (client instances), {@code short-lived-client} (a new
 *                    client for every read, shut down after it) or {@code member} (the map proxies of
 *                    the members that do not own the hot key)
 * @param routing     the clients' routing mode: {@code ALL_MEMBERS} or {@code SINGLE_MEMBER}
 * @param keyStore    a PKCS12 key store for TLS between all members and clients, or {@code null}
 * @param valueSize   bytes of the hot value
 * @param members     members in the cluster, at least 2
 * @param readers     client instances; with {@code --caller member}, the reads are spread over the
 *                    members that do not own the hot key
 * @param concurrency reads in flight at any time, over all clients
 * @param sync        blocking {@code get}s on {@code concurrency} application threads instead of
 *                    {@code getAsync} with a callback
 * @param gets        measured reads
 * @param warmUpGets  reads before the measurement
 * @param out         directory for the recording and the report
 * @param maxCopies   fail if member threads allocate more value-sized objects per read than this;
 *                    negative to only report
 */
record Scenario(String product, String caller, String format, String nearCache, String routing, Path keyStore, int valueSize, int members,
                int readers, int concurrency, boolean sync, int gets, int warmUpGets, Path out, double maxCopies) {

    static final String KEY_STORE_PASSWORD = "changeit";

    static Scenario parse(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("unexpected argument " + arg);
            }
            String name = arg.substring(2);
            if (name.equals("help") || name.equals("sync")) {
                options.put(name, "true");
            } else if (i + 1 < args.length) {
                options.put(name, args[++i]);
            } else {
                throw new IllegalArgumentException("missing value for " + arg);
            }
        }
        if (options.containsKey("help")) {
            throw new IllegalArgumentException("usage");
        }
        String product = options.getOrDefault("product", "hecaton");
        String keyStore = options.get("key-store");
        if (keyStore != null && product.equals("hazelcast")) {
            throw new IllegalArgumentException("TLS is not part of Hazelcast CE; --key-store needs --product hecaton");
        }
        Scenario scenario = new Scenario(product,
                options.getOrDefault("caller", "client"),
                options.getOrDefault("format", "BINARY").toUpperCase(Locale.ROOT),
                options.containsKey("near-cache") ? options.get("near-cache").toUpperCase(Locale.ROOT) : null,
                options.getOrDefault("routing", "ALL_MEMBERS").toUpperCase(Locale.ROOT),
                keyStore == null ? null : Path.of(keyStore),
                (int) bytes(options.getOrDefault("value-size", "1m")),
                Integer.parseInt(options.getOrDefault("members", "2")),
                Integer.parseInt(options.getOrDefault("readers", "8")),
                Integer.parseInt(options.getOrDefault("concurrency", "256")),
                options.containsKey("sync"),
                Integer.parseInt(options.getOrDefault("gets", "2000")),
                Integer.parseInt(options.getOrDefault("warm-up-gets", "500")),
                Path.of(options.getOrDefault("out", "target/fanout")),
                Double.parseDouble(options.getOrDefault("max-copies-per-read", "-1")));
        if (scenario.members < 2) {
            throw new IllegalArgumentException("--members must be at least 2");
        }
        if (!List.of("client", "short-lived-client", "member").contains(scenario.caller)) {
            throw new IllegalArgumentException("--caller must be client, short-lived-client or member");
        }
        return scenario;
    }

    boolean tls() {
        return keyStore != null;
    }

    boolean memberCaller() {
        return caller.equals("member");
    }

    boolean shortLivedClients() {
        return caller.equals("short-lived-client");
    }

    /** A short name for files and headings, e.g. {@code hecaton-BINARY-ALL_MEMBERS-tls-1m}. */
    String name() {
        String callers = memberCaller() ? "member-caller" : shortLivedClients() ? "short-lived-" + routing : routing;
        return product + "-" + format + (nearCache == null ? "" : "-near-cache-" + nearCache) + "-" + callers
                + (sync ? "-sync" : "") + (tls() ? "-tls" : "") + "-" + size(valueSize);
    }

    static long bytes(String text) {
        String lower = text.toLowerCase(Locale.ROOT);
        long unit = switch (lower.charAt(lower.length() - 1)) {
            case 'k' -> 1L << 10;
            case 'm' -> 1L << 20;
            case 'g' -> 1L << 30;
            default -> 1;
        };
        String digits = unit == 1 ? lower : lower.substring(0, lower.length() - 1);
        return Long.parseLong(digits) * unit;
    }

    static String size(long bytes) {
        if (bytes >= 1L << 30 && bytes % (1L << 30) == 0) {
            return (bytes >> 30) + "g";
        } else if (bytes >= 1L << 20 && bytes % (1L << 20) == 0) {
            return (bytes >> 20) + "m";
        } else if (bytes >= 1L << 10 && bytes % (1L << 10) == 0) {
            return (bytes >> 10) + "k";
        }
        return Long.toString(bytes);
    }
}
