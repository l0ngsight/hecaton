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

package uk.org.hecaton.jet.clickhouse;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.config.MapStoreConfig.InitialLoadMode;
import uk.org.hecaton.jet.SimpleTestInClusterSupport;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecord;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecordBuilder;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import com.clickhouse.client.api.Client;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.math.BigInteger;
import java.util.List;
import java.util.Properties;

import static uk.org.hecaton.jet.clickhouse.ClickHouseTestSupport.CLICKHOUSE_IMAGE;
import static uk.org.hecaton.jet.clickhouse.ClickHouseTestSupport.dataConnectionConfig;
import static uk.org.hecaton.jet.clickhouse.ClickHouseTestSupport.endpoint;
import static uk.org.hecaton.jet.clickhouse.ClickHouseTestSupport.execute;
import static uk.org.hecaton.jet.clickhouse.ClickHouseTestSupport.newClient;
import static uk.org.hecaton.jet.clickhouse.ClickHouseTestSupport.newServer;
import static uk.org.hecaton.jet.clickhouse.ClickHouseTestSupport.query;
import static uk.org.hecaton.test.DockerTestUtil.assumeDockerEnabled;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The map store on a {@code ReplicatedReplacingMergeTree} table with two replicas and ClickHouse
 * Keeper: members write through one replica and read through the other.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class ClickHouseReplicatedMapStoreTest extends SimpleTestInClusterSupport {

    private static final DockerImageName KEEPER_IMAGE = DockerImageName.parse("clickhouse/clickhouse-keeper:"
            + System.getProperty("test.clickhouse.version", "25.8"));
    private static final String KEEPER_CONFIG = """
            <clickhouse>
                <listen_host>0.0.0.0</listen_host>
                <logger><level>warning</level><console>true</console></logger>
                <keeper_server>
                    <tcp_port>9181</tcp_port>
                    <server_id>1</server_id>
                    <enable_ipv6>false</enable_ipv6>
                    <log_storage_path>/var/lib/clickhouse-keeper/log</log_storage_path>
                    <snapshot_storage_path>/var/lib/clickhouse-keeper/snapshots</snapshot_storage_path>
                    <raft_configuration>
                        <server><id>1</id><hostname>keeper</hostname><port>9234</port></server>
                    </raft_configuration>
                </keeper_server>
            </clickhouse>
            """;
    private static final String SERVER_CONFIG = """
            <clickhouse>
                <zookeeper><node><host>keeper</host><port>9181</port></node></zookeeper>
                <macros><shard>1</shard><replica>%s</replica></macros>
                <interserver_http_host>%s</interserver_http_host>
            </clickhouse>
            """;
    private static final String TABLE = "people";

    private static Network network;
    private static GenericContainer<?> keeper;
    private static GenericContainer<?> replica1;
    private static GenericContainer<?> replica2;
    private static Client client1;
    private static Client client2;

    @BeforeClass
    @SuppressWarnings("resource")
    public static void beforeClass() {
        assumeDockerEnabled();
        network = Network.newNetwork();
        keeper = new GenericContainer<>(KEEPER_IMAGE)
                .withNetwork(network)
                .withNetworkAliases("keeper")
                .withCopyToContainer(Transferable.of(KEEPER_CONFIG), "/etc/clickhouse-keeper/keeper_config.xml")
                .withExposedPorts(9181)
                .waitingFor(Wait.forListeningPort());
        keeper.start();
        replica1 = replica("replica1");
        replica2 = replica("replica2");
        replica1.start();
        replica2.start();
        client1 = newClient(endpoint(replica1));
        client2 = newClient(endpoint(replica2));
        for (Client client : List.of(client1, client2)) {
            // the replicas connect to Keeper in the background after they start
            assertTrueEventually(() -> execute(client, "CREATE TABLE IF NOT EXISTS " + TABLE
                    + " (id Int64, name String, version UInt64, is_deleted UInt8) ENGINE = "
                    + "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/people', '{replica}', "
                    + "version, is_deleted) ORDER BY id"));
        }

        Config config = smallInstanceConfig();
        config.addDataConnectionConfig(dataConnectionConfig("replica1", endpoint(replica1)));
        config.addDataConnectionConfig(dataConnectionConfig("replica2", endpoint(replica2)));
        // writes through replica 1, acknowledged by both replicas
        config.addMapConfig(mapConfig("written", "replica1", ClickHouseMapStore.INSERT_QUORUM_PROPERTY, "2"));
        // reads through replica 2, only what all quorum inserts wrote
        config.addMapConfig(mapConfig("read", "replica2",
                ClickHouseMapStore.SELECT_SEQUENTIAL_CONSISTENCY_PROPERTY, "true"));
        initialize(2, config);
    }

    @SuppressWarnings("resource")
    private static GenericContainer<?> replica(String name) {
        return newServer(CLICKHOUSE_IMAGE)
                .withNetwork(network)
                .withNetworkAliases(name)
                .withCopyToContainer(Transferable.of(SERVER_CONFIG.formatted(name, name)),
                        "/etc/clickhouse-server/config.d/replication.xml");
    }

    private static MapConfig mapConfig(String mapName, String dataConnection, String property, String value) {
        Properties properties = new Properties();
        properties.setProperty(ClickHouseMapStore.DATA_CONNECTION_REF_PROPERTY, dataConnection);
        properties.setProperty(ClickHouseMapStore.EXTERNAL_NAME_PROPERTY, TABLE);
        properties.setProperty(ClickHouseMapStore.TYPE_NAME_PROPERTY, "person");
        properties.setProperty(property, value);
        return new MapConfig(mapName).setMapStoreConfig(new MapStoreConfig()
                .setEnabled(true)
                .setInitialLoadMode(InitialLoadMode.LAZY)
                .setClassName(ClickHouseMapStore.class.getName())
                .setProperties(properties));
    }

    @AfterClass
    public static void afterClass() {
        for (AutoCloseable closeable : new AutoCloseable[]{client1, client2, replica1, replica2, keeper, network}) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (Exception ignored) {
                    // best effort
                }
            }
        }
    }

    @Test
    public void readsThroughOneReplicaWhatWasWrittenThroughTheOther() {
        IMap<Long, GenericRecord> written = instance().getMap("written");
        IMap<Long, GenericRecord> read = instance().getMap("read");

        for (long i = 0; i < 100; i++) {
            written.put(i, GenericRecordBuilder.compact("person").setString("name", "v1-" + i).build());
        }
        written.put(7L, GenericRecordBuilder.compact("person").setString("name", "v2-7").build());
        written.delete(8L);

        // no waiting: the quorum inserts are on replica 2 once put returns
        assertThat(read.get(1L).getString("name")).isEqualTo("v1-1");
        assertThat(read.get(7L).getString("name")).isEqualTo("v2-7");
        assertThat(read.get(8L)).isNull();
        assertThat(query(client2, "SELECT count() FROM " + TABLE + " FINAL WHERE is_deleted = 0").get(0).get(0))
                .isEqualTo(BigInteger.valueOf(99));
    }
}
