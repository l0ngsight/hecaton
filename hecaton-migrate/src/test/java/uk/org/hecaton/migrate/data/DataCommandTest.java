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
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecord;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecordBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DataCommandTest {

    private static HecatonInstance source;
    private static HecatonInstance target;
    private static String sourceAddress;
    private static String targetAddress;

    @BeforeAll
    static void startClusters() {
        source = member("copy-source", 6011);
        target = member("copy-target", 6021);
        sourceAddress = "127.0.0.1:" + source.getCluster().getLocalMember().getAddress().getPort();
        targetAddress = "127.0.0.1:" + target.getCluster().getLocalMember().getAddress().getPort();

        HecatonInstance client = client("copy-source", sourceAddress);
        try {
            IMap<String, Integer> plain = client.getMap("plain");
            for (int i = 0; i < 2500; i++) {
                plain.put("key-" + i, i);
            }
            IMap<Long, GenericRecord> orders = client.getMap("orders");
            for (long id = 1; id <= 3; id++) {
                orders.put(id, GenericRecordBuilder.compact("Order")
                        .setInt64("id", id).setString("customer", "customer-" + id).build());
            }
            client.getMultiMap("tags").put("a", "x");
            client.getMultiMap("tags").put("a", "y");
            client.getReplicatedMap("settings").put("mode", "fast");
            client.getMap("__internal").put("k", "v");
        } finally {
            client.shutdown();
        }
    }

    @AfterAll
    static void stopClusters() {
        Hecaton.shutdownAll();
        HecatonClient.shutdownAll();
    }

    @Test
    void copiesEverythingButInternalStructures() throws Exception {
        String output = run("data", "--source", sourceAddress, "--source-cluster", "copy-source",
                "--target", targetAddress, "--target-cluster", "copy-target", "--batch", "100");

        assertThat(output).contains("copied 2500 entries of map plain", "copied 3 entries of map orders",
                "copied 2 entries of multimap tags", "copied 1 entries of replicated map settings");
        HecatonInstance client = client("copy-target", targetAddress);
        try {
            assertThat(client.getMap("plain").size()).isEqualTo(2500);
            assertThat(client.getMap("plain").get("key-2499")).isEqualTo(2499);
            GenericRecord order = (GenericRecord) client.getMap("orders").get(2L);
            assertThat(order.getString("customer")).isEqualTo("customer-2");
            assertThat(client.<String, String>getMultiMap("tags").get("a")).containsExactlyInAnyOrder("x", "y");
            assertThat(client.getReplicatedMap("settings").get("mode")).isEqualTo("fast");
            assertThat(client.getMap("__internal").size()).isZero();
        } finally {
            client.shutdown();
        }
    }

    @Test
    void copiesNamedStructuresAndReportsMissingOnes() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(out, true, StandardCharsets.UTF_8);
        int status = DataCommand.run(List.of("--source", sourceAddress, "--source-cluster", "copy-source",
                "--target", targetAddress, "--target-cluster", "copy-target",
                "--name", "settings", "--name", "missing"), stream, stream);

        assertThat(status).isEqualTo(1);
        assertThat(out.toString(StandardCharsets.UTF_8))
                .contains("copied 1 entries of replicated map settings")
                .doesNotContain("map plain")
                .contains("Not found in the source: [missing]");
    }

    private static String run(String... args) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(out, true, StandardCharsets.UTF_8);
        int status = uk.org.hecaton.migrate.MainAccess.run(args, stream);
        assertThat(status).as(out.toString(StandardCharsets.UTF_8)).isZero();
        return out.toString(StandardCharsets.UTF_8);
    }

    private static HecatonInstance member(String clusterName, int port) {
        Config config = new Config().setClusterName(clusterName);
        config.getNetworkConfig().setPort(port);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        return Hecaton.newHecatonInstance(config);
    }

    private static HecatonInstance client(String clusterName, String address) {
        ClientConfig config = new ClientConfig().setClusterName(clusterName);
        config.getNetworkConfig().addAddress(address);
        return HecatonClient.newHecatonClient(config);
    }
}
