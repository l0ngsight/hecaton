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

package uk.org.hecaton.migrate.data;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.XmlClientConfigBuilder;
import uk.org.hecaton.client.config.YamlClientConfigBuilder;
import uk.org.hecaton.core.HecatonInstance;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * {@code hecaton-migrate data}: copies maps from a Hazelcast cluster to a Hecaton cluster.
 *
 * <pre>
 * hecaton-migrate data --source SOURCE --target TARGET [--source-cluster NAME] [--target-cluster NAME]
 *                      [--name NAME]... [--batch N]
 * </pre>
 * SOURCE and TARGET are a client configuration file or comma-separated member addresses.
 */
public final class DataCommand {

    /** Help text. */
    public static final String USAGE = """
            Usage: hecaton-migrate data --source SOURCE --target TARGET [OPTIONS]

            Copies IMap, MultiMap and ReplicatedMap data from a Hazelcast 5.x cluster (SOURCE)
            to a Hecaton cluster (TARGET) through two clients. SOURCE and TARGET are a client
            configuration file (XML or YAML) or comma-separated member addresses. Stop writes to
            the source first for an exact copy.

              --source-cluster NAME  cluster name of SOURCE given as addresses (default: dev)
              --target-cluster NAME  cluster name of TARGET given as addresses (default: dev)
              --name NAME            copy only this structure (repeatable; default: all but internal ones)
              --batch N              entries per request (default: 1000)
            """;

    private static final int DEFAULT_BATCH = 1000;

    private DataCommand() {
    }

    /** Runs the command; returns the exit status. */
    public static int run(List<String> args, PrintStream out, PrintStream err) throws IOException {
        String source = null;
        String target = null;
        String sourceCluster = "dev";
        String targetCluster = "dev";
        int batch = DEFAULT_BATCH;
        Set<String> names = new LinkedHashSet<>();
        for (int i = 0; i < args.size(); i++) {
            String arg = args.get(i);
            if (i + 1 >= args.size()) {
                err.println("Missing value for " + arg);
                err.print(USAGE);
                return 2;
            }
            String value = args.get(++i);
            switch (arg) {
                case "--source" -> source = value;
                case "--target" -> target = value;
                case "--source-cluster" -> sourceCluster = value;
                case "--target-cluster" -> targetCluster = value;
                case "--name" -> names.add(value);
                case "--batch" -> batch = Integer.parseInt(value);
                default -> {
                    err.println("Unknown option: " + arg);
                    err.print(USAGE);
                    return 2;
                }
            }
        }
        if (source == null || target == null) {
            err.print(USAGE);
            return 2;
        }
        return copy(clientConfig(source, sourceCluster), clientConfig(target, targetCluster), batch, names, out, err);
    }

    private static int copy(ClientConfig sourceConfig, ClientConfig targetConfig, int batch, Set<String> names,
                            PrintStream out, PrintStream err) {
        HecatonInstance sourceClient = HecatonClient.newHecatonClient(sourceConfig);
        try {
            HecatonInstance targetClient = HecatonClient.newHecatonClient(targetConfig);
            try {
                List<DataCopier.Copied> copied = new DataCopier(sourceClient, targetClient, batch, out).copy(names);
                out.println("Copied " + copied.size() + " structure(s), "
                        + copied.stream().mapToLong(DataCopier.Copied::entries).sum() + " entries.");
                if (!names.isEmpty() && copied.size() < names.size()) {
                    err.println("Not found in the source: " + names.stream()
                            .filter(n -> copied.stream().noneMatch(c -> c.name().equals(n))).toList());
                    return 1;
                }
                return 0;
            } finally {
                targetClient.shutdown();
            }
        } finally {
            sourceClient.shutdown();
        }
    }

    static ClientConfig clientConfig(String spec, String clusterName) throws IOException {
        Path file = Path.of(spec);
        if (Files.isRegularFile(file)) {
            String name = file.getFileName().toString();
            return name.endsWith(".yaml") || name.endsWith(".yml")
                    ? new YamlClientConfigBuilder(file.toFile()).build()
                    : new XmlClientConfigBuilder(file.toFile()).build();
        }
        ClientConfig config = new ClientConfig().setClusterName(clusterName);
        for (String address : spec.split(",")) {
            config.getNetworkConfig().addAddress(address.strip());
        }
        return config;
    }
}
