/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.mapstore;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.DataConnectionConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.sql.impl.connector.jdbc.JdbcSqlTestSupport;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.test.jdbc.H2DatabaseProvider;
import uk.org.hecaton.test.jdbc.TestDatabaseProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.HashMap;
import java.util.LinkedHashMap;

import java.util.Locale;
import java.util.Map;

import static uk.org.hecaton.mapstore.GenericMapLoader.EXTERNAL_NAME_PROPERTY;
import static uk.org.hecaton.mapstore.GenericMapLoader.LOAD_ALL_KEYS_PROPERTY;
import static uk.org.hecaton.mapstore.GenericMapStore.DATA_CONNECTION_REF_PROPERTY;
import static uk.org.hecaton.mapstore.GenericMapStore.SINGLE_COLUMN_AS_VALUE;
import static uk.org.hecaton.test.DockerTestUtil.assumeDockerEnabled;
import static org.assertj.core.api.Assertions.assertThat;

@QuickTest
public class SingleColumnGenericMapStoreIntegrationTest extends JdbcSqlTestSupport {

    private String tableName;

    @BeforeAll
    public static void beforeClass() {
        assumeDockerEnabled();
        initializeBeforeClass(new H2DatabaseProvider());
    }

    protected static void initializeBeforeClass(TestDatabaseProvider testDatabaseProvider) {
        databaseProvider = testDatabaseProvider;
        dbConnectionUrl = databaseProvider.createDatabase(JdbcSqlTestSupport.class.getName());

        // Do not use small config to run with more threads and partitions
        Config memberConfig = new Config()
                .addDataConnectionConfig(
                        new DataConnectionConfig(TEST_DATABASE_REF)
                                .setType("jdbc")
                                .setProperty("jdbcUrl", dbConnectionUrl)
                                        );
        memberConfig.getJetConfig().setEnabled(true);

        ClientConfig clientConfig = new ClientConfig();

        initializeWithClient(2, memberConfig, clientConfig);
        sqlService = instance().getSql();
    }

    @BeforeEach
    public void setUp(TestInfo testInfo) throws Exception {
        tableName =  "generic_" + testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);
        createTable(tableName);
        insertItems(tableName, 1);

        MapConfig mapConfig = new MapConfig(tableName);
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setClassName(GenericMapStore.class.getName());
        mapStoreConfig.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        mapStoreConfig.setProperty(SINGLE_COLUMN_AS_VALUE, "true");
        mapConfig.setMapStoreConfig(mapStoreConfig);
        instance().getConfig().addMapConfig(mapConfig);
    }

    @Test
    public void testLoadAllKeys() {
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setClassName(GenericMapStore.class.getName());
        mapStoreConfig.setProperty(DATA_CONNECTION_REF_PROPERTY, TEST_DATABASE_REF);
        mapStoreConfig.setProperty(EXTERNAL_NAME_PROPERTY, tableName);
        mapStoreConfig.setProperty(SINGLE_COLUMN_AS_VALUE, "true");
        mapStoreConfig.setProperty(LOAD_ALL_KEYS_PROPERTY, "false");

        String mapName = tableName + "_disabled";
        MapConfig mapConfig = new MapConfig(mapName);

        mapConfig.setMapStoreConfig(mapStoreConfig);
        instance().getConfig().addMapConfig(mapConfig);

        HecatonInstance client = client();
        IMap<Integer, String> map = client.getMap(mapName);
        assertThat(map.size()).isZero();

        // LOAD_ALL_KEYS_PROPERTY is disabled, but we still can load
        String name = map.get(0);
        assertThat(name).isEqualTo("name-0");
    }

    @Test
    public void testGet() {
        HecatonInstance client = client();
        IMap<Integer, String> map = client.getMap(tableName);

        String name = map.get(0);
        assertThat(name).isEqualTo("name-0");
    }

    @Test
    public void testPut() {
        HecatonInstance client = client();
        IMap<Integer, String> map = client.getMap(tableName);

        map.put(42, "name-42");

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, "name-0"),
                new Row(42, "name-42")
        );
    }

    @Test
    public void testPutWhenExists() {
        HecatonInstance client = client();
        IMap<Integer, String> map = client.getMap(tableName);

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, "name-0")
        );

        map.put(0, "updated");

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, "updated")
        );
    }

    @Test
    public void testRemove() {
        HecatonInstance client = client();
        IMap<Integer, String> map = client.getMap(tableName);

        assertThat(jdbcRowsTable(tableName)).hasSize(1);

        map.remove(0);

        assertThat(jdbcRowsTable(tableName)).isEmpty();
    }

    @Test
    public void testRemoveWhenNotExists() {
        HecatonInstance client = client();
        IMap<Integer, String> map = client.getMap(tableName);

        assertThat(jdbcRowsTable(tableName)).hasSize(1);

        assertThat(map.remove(1)).isNull();

        assertThat(jdbcRowsTable(tableName)).hasSize(1);
    }

    @Test
    public void testRemoveWhenExistsInTableOnly() {
        HecatonInstance client = client();
        IMap<Integer, String> map = client.getMap(tableName);

        map.put(42, "name-42");
        map.evictAll();

        assertThat(map.size()).isZero();
        assertThat(jdbcRowsTable(tableName)).hasSize(2);

        String name = map.remove(0);

        assertThat(name).isEqualTo("name-0");

        assertThat(jdbcRowsTable(tableName)).hasSize(1);
    }

    @Test
    public void testPutAll() {
        HecatonInstance client = client();
        IMap<Integer, String> map = client.getMap(tableName);

        Map<Integer, String> putMap = new HashMap<>();
        putMap.put(42, "name-42");
        putMap.put(43, "name-43");
        putMap.put(44, "name-44");
        map.putAll(putMap);

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, "name-0"),
                new Row(42, "name-42"),
                new Row(43, "name-43"),
                new Row(44, "name-44")
        );
    }

    @Test
    public void testPutAllWhenExists() {
        HecatonInstance client = client();
        IMap<Integer, String> map = client.getMap(tableName);

        Map<Integer, String> putMap = new LinkedHashMap<>();
        putMap.put(42, "name-42");
        putMap.put(0, "updated");
        putMap.put(44, "name-44");
        map.putAll(putMap);

        assertJdbcRowsAnyOrder(tableName,
                new Row(0, "updated"),
                new Row(42, "name-42"),
                new Row(44, "name-44")
        );
    }
}
