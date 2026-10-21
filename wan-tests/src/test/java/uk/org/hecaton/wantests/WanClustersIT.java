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

package uk.org.hecaton.wantests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Three clusters of the Hecaton image, one container per member on the host network: A
 * replicates to B (active/passive), A and C replicate to each other (active/active), and A loses a
 * member while it replicates.
 */
class WanClustersIT {

    private static final String IMAGE = System.getProperty("hecaton.image");
    private static final int[] A = {6701, 6702};
    private static final int[] B = {6801, 6802};
    private static final int[] C = {6901};
    private static final int ENTRIES = 2000;
    private static final Duration TIMEOUT = Duration.ofMinutes(2);

    private final List<GenericContainer<?>> members = new ArrayList<>();
    private final List<HecatonInstance> clients = new ArrayList<>();

    @AfterEach
    void tearDown() {
        clients.forEach(HecatonInstance::shutdown);
        members.forEach(GenericContainer::stop);
    }

    @Test
    void replicatesBetweenThreeClusters() throws Exception {
        List<GenericContainer<?>> a = startCluster("a", A, publisher("b", B) + publisher("c", C));
        startCluster("b", B, "");
        startCluster("c", C, publisher("a", A));

        IMap<Integer, String> mapA = client("a", A).getMap("orders");
        IMap<Integer, String> mapB = client("b", B).getMap("orders");
        IMap<Integer, String> mapC = client("c", C).getMap("orders");

        for (int i = 0; i < ENTRIES; i++) {
            mapA.put(i, "a" + i);
        }
        for (int i = ENTRIES; i < 2 * ENTRIES; i++) {
            mapC.put(i, "c" + i);
        }
        // A to B and C; C to A
        awaitSize(mapB, ENTRIES);
        awaitSize(mapC, 2 * ENTRIES);
        awaitSize(mapA, 2 * ENTRIES);
        // what A received from C is not replicated to B
        Thread.sleep(2000);
        assertThat(mapB.size()).isEqualTo(ENTRIES);

        // a member of A fails while A replicates: the other sends the events of its backups
        for (int i = 0; i < ENTRIES; i++) {
            mapA.put(i, "a" + i + "'");
        }
        a.get(1).getDockerClient().killContainerCmd(a.get(1).getContainerId()).exec();
        await(() -> IntStream.range(0, ENTRIES).allMatch(i -> ("a" + i + "'").equals(mapB.get(i))));
        assertThat(mapC.get(0)).isEqualTo("a0'");
    }

    private List<GenericContainer<?>> startCluster(String name, int[] ports, String publishers) {
        List<GenericContainer<?>> cluster = new ArrayList<>();
        for (int port : ports) {
            String config = memberConfig(name, port, ports, publishers);
            GenericContainer<?> member = new GenericContainer<>(IMAGE)
                    .withNetworkMode("host")
                    .withEnv("HECATON_CONFIG", "config/wan.yaml")
                    .withEnv("JAVA_OPTS", "-Xmx256m")
                    .withCopyToContainer(Transferable.of(config), "/opt/hecaton/config/wan.yaml")
                    .waitingFor(Wait.forLogMessage(".*is STARTED.*", 1).withStartupTimeout(TIMEOUT));
            member.start();
            members.add(member);
            cluster.add(member);
        }
        return cluster;
    }

    private static String memberConfig(String name, int port, int[] ports, String publishers) {
        String wan = publishers.isEmpty() ? "" : """
                  wan-replication:
                    out:
                      batch-publisher:
                %s
                  map:
                    orders:
                      wan-replication-ref:
                        out:
                          merge-policy-class-name: PassThroughMergePolicy
                """.formatted(publishers);
        return """
                hecaton:
                  cluster-name: %s
                  network:
                    port:
                      port: %d
                      auto-increment: false
                    interfaces:
                      enabled: true
                      interfaces:
                        - 127.0.0.1
                    join:
                      auto-detection:
                        enabled: false
                      multicast:
                        enabled: false
                      tcp-ip:
                        enabled: true
                        member-list: [%s]
                  jet:
                    enabled: false
                """.formatted(name, port, addresses(ports)) + wan;
    }

    private static String publisher(String cluster, int[] ports) {
        return """
                        %s:
                          cluster-name: %s
                          target-endpoints: %s
                          batch-max-delay-millis: 100
                """.formatted(cluster, cluster, addresses(ports));
    }

    private HecatonInstance client(String cluster, int[] ports) {
        ClientConfig config = new ClientConfig().setClusterName(cluster);
        for (int port : ports) {
            config.getNetworkConfig().addAddress("127.0.0.1:" + port);
        }
        HecatonInstance client = HecatonClient.newHecatonClient(config);
        clients.add(client);
        return client;
    }

    private static String addresses(int[] ports) {
        return IntStream.of(ports).mapToObj(port -> "127.0.0.1:" + port).collect(Collectors.joining(","));
    }

    private static void awaitSize(IMap<?, ?> map, int size) throws InterruptedException {
        await(() -> map.size() == size);
        assertThat(map.size()).isEqualTo(size);
    }

    private static void await(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + TIMEOUT);
            }
            Thread.sleep(500);
        }
    }
}
