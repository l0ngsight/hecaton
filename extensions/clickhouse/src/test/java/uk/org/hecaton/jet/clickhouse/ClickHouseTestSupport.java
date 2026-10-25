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
import uk.org.hecaton.config.DataConnectionConfig;
import uk.org.hecaton.jet.SimpleTestInClusterSupport;
import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static uk.org.hecaton.test.DockerTestUtil.assumeDockerEnabled;

/**
 * Starts one ClickHouse server for the test class, and gives the members a data connection
 * {@value #DATA_CONNECTION} to it.
 */
public abstract class ClickHouseTestSupport extends SimpleTestInClusterSupport {

    static final DockerImageName CLICKHOUSE_IMAGE = DockerImageName.parse("clickhouse/clickhouse-server:"
            + System.getProperty("test.clickhouse.version", "25.8"));
    static final String USER = "hecaton";
    static final String PASSWORD = "hecaton-test";
    static final String DATA_CONNECTION = "clickhouse";
    static final int HTTP_PORT = 8123;

    protected static GenericContainer<?> clickhouse;
    protected static Client client;

    private static final AtomicInteger TABLE_COUNTER = new AtomicInteger();

    @BeforeClass
    public static void startClickHouse() {
        assumeDockerEnabled();
        clickhouse = newServer(CLICKHOUSE_IMAGE);
        clickhouse.start();
        client = newClient(endpoint(clickhouse));
    }

    @AfterClass
    public static void stopClickHouse() {
        if (client != null) {
            client.close();
            client = null;
        }
        if (clickhouse != null) {
            clickhouse.stop();
            clickhouse = null;
        }
    }

    @SuppressWarnings("resource")
    static GenericContainer<?> newServer(DockerImageName image) {
        return new GenericContainer<>(image)
                .withEnv("CLICKHOUSE_USER", USER)
                .withEnv("CLICKHOUSE_PASSWORD", PASSWORD)
                .withEnv("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
                .withExposedPorts(HTTP_PORT)
                .waitingFor(Wait.forHttp("/ping").forPort(HTTP_PORT).forStatusCode(200));
    }

    static String endpoint(GenericContainer<?> server) {
        return "http://" + server.getHost() + ":" + server.getMappedPort(HTTP_PORT);
    }

    static Client newClient(String endpoint) {
        return new Client.Builder().addEndpoint(endpoint).setUsername(USER).setPassword(PASSWORD).build();
    }

    static DataConnectionConfig dataConnectionConfig(String name, String endpoints) {
        return new DataConnectionConfig(name)
                .setType(ClickHouseDataConnection.TYPE)
                .setShared(true)
                .setProperty(ClickHouseDataConnection.ENDPOINTS_PROPERTY, endpoints)
                .setProperty(ClickHouseDataConnection.USERNAME_PROPERTY, USER)
                .setProperty(ClickHouseDataConnection.PASSWORD_PROPERTY, PASSWORD);
    }

    /**
     * Starts two members with the data connection {@value #DATA_CONNECTION}.
     */
    static void initializeMembers() {
        Config config = smallInstanceConfig();
        config.getJetConfig().setEnabled(true);
        config.addDataConnectionConfig(dataConnectionConfig(DATA_CONNECTION, endpoint(clickhouse)));
        initialize(2, config);
    }

    static String newTableName(String prefix) {
        return prefix + "_" + TABLE_COUNTER.incrementAndGet();
    }

    static void execute(String sql) {
        execute(client, sql);
    }

    static void execute(Client client, String sql) {
        try {
            client.execute(sql).get().close();
        } catch (Exception e) {
            throw new AssertionError("Failed: " + sql, e);
        }
    }

    /**
     * Runs a query and returns its rows, each value as the client reads it.
     */
    static List<List<Object>> query(String sql) {
        return query(client, sql);
    }

    static List<List<Object>> query(Client client, String sql) {
        try (QueryResponse response = client.query(sql).get();
             ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
            List<List<Object>> rows = new ArrayList<>();
            int columns = reader.getSchema().getColumns().size();
            while (reader.hasNext()) {
                reader.next();
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= columns; i++) {
                    row.add(reader.readValue(i));
                }
                rows.add(row);
            }
            return rows;
        } catch (Exception e) {
            throw new AssertionError("Failed: " + sql, e);
        }
    }

    static long count(String sql) {
        return ((Number) query(sql).get(0).get(0)).longValue();
    }
}
