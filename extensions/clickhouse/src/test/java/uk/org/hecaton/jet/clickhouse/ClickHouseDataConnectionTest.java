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

import uk.org.hecaton.config.DataConnectionConfig;
import uk.org.hecaton.dataconnection.DataConnectionResource;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import com.clickhouse.client.api.Client;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class ClickHouseDataConnectionTest extends ClickHouseTestSupport {

    @Test
    public void sharedConnectionHasOneClient() {
        ClickHouseDataConnection connection = new ClickHouseDataConnection(
                dataConnectionConfig("shared", endpoint(clickhouse)));
        try {
            Client first = connection.getClient();
            assertThat(connection.getClient()).isSameAs(first);
            assertThat(first.ping()).isTrue();
        } finally {
            connection.destroy();
        }
        assertThatThrownBy(connection::getClient).hasMessageContaining("is closed");
    }

    @Test
    public void unsharedConnectionCreatesAClientPerCall() {
        DataConnectionConfig config = dataConnectionConfig("unshared", endpoint(clickhouse)).setShared(false);
        ClickHouseDataConnection connection = new ClickHouseDataConnection(config);
        try (Client first = connection.getClient(); Client second = connection.getClient()) {
            assertThat(second).isNotSameAs(first);
            assertThat(first.ping()).isTrue();
        } finally {
            connection.destroy();
        }
    }

    @Test
    public void listsTablesOfTheDefaultDatabase() {
        execute("CREATE DATABASE IF NOT EXISTS listing");
        execute("CREATE TABLE IF NOT EXISTS listing.t1 (id Int64) ENGINE = MergeTree ORDER BY id");
        execute("CREATE TABLE IF NOT EXISTS listing.t2 (id Int64) ENGINE = MergeTree ORDER BY id");
        ClickHouseDataConnection connection = new ClickHouseDataConnection(
                dataConnectionConfig("listing", endpoint(clickhouse))
                        .setProperty(ClickHouseDataConnection.DATABASE_PROPERTY, "listing"));
        try {
            List<DataConnectionResource> resources = connection.listResources();
            assertThat(resources).containsExactly(
                    new DataConnectionResource("Table", "listing", "t1"),
                    new DataConnectionResource("Table", "listing", "t2"));
            assertThat(connection.resourceTypes()).containsExactly("Table");
        } finally {
            connection.destroy();
        }
    }

    @Test
    public void passesClientOptions() {
        ClickHouseDataConnection connection = new ClickHouseDataConnection(
                dataConnectionConfig("options", endpoint(clickhouse))
                        .setProperty("client.compress", "false"));
        try {
            assertThat(connection.getClient().getConfiguration()).containsEntry("compress", "false");
        } finally {
            connection.destroy();
        }
    }

    @Test
    public void needsEndpoints() {
        assertThatThrownBy(() -> new ClickHouseDataConnection(new DataConnectionConfig("none").setType("ClickHouse")))
                .hasMessageContaining("needs the property 'endpoints'");
    }
}
