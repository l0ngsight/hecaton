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

package uk.org.hecaton.jet.sql.impl.type;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.serialization.impl.compact.DeserializedGenericRecord;
import uk.org.hecaton.jet.sql.SqlTestSupport;
import uk.org.hecaton.jet.sql.impl.connector.map.IMapSqlConnector;
import uk.org.hecaton.sql.SqlResult;
import uk.org.hecaton.sql.SqlRow;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;

import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.COMPACT_FORMAT;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_COMPACT_TYPE_NAME;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
public class CompactNestedFieldsTest extends SqlTestSupport {

    @BeforeClass
    public static void beforeClass() {
        final Config config = new Config();
        config.getJetConfig().setEnabled(true);
        initializeWithClient(3, config, null);
    }

    static void setupCompactTypesForNestedQuery(HecatonInstance instance) {
        new SqlType("Office").fields("id BIGINT", "name VARCHAR").create(instance);
        new SqlType("Organization").fields("id BIGINT", "name VARCHAR", "office Office").create(instance);

        createCompactMapping(instance, "test", "UserCompactType",
                "id BIGINT", "name VARCHAR", "organization Organization");
    }

    static void createCompactMapping(HecatonInstance instance, String name, String valueCompactTypeName,
                                     String... valueFields) {
        new SqlMapping(name, IMapSqlConnector.class)
                .fields("__key BIGINT")
                .fields(valueFields)
                .options(OPTION_KEY_FORMAT, "bigint",
                         OPTION_VALUE_FORMAT, COMPACT_FORMAT,
                         OPTION_VALUE_COMPACT_TYPE_NAME, valueCompactTypeName)
                .create(instance);
    }

    private static void createType(String name, String... fields) {
        new SqlType(name)
                .fields(fields)
                .create(client());
    }

    private static SqlResult execute(String sql) {
        return client().getSql().execute(sql);
    }

    @Test
    public void test_basicQuerying() {
        setupCompactTypesForNestedQuery(client());
        execute("INSERT INTO test VALUES (1, 1, 'user1', (1, 'organization1', (1, 'office1')))");
        assertRowsAnyOrder("SELECT (organization).office.name FROM test", rows(1, "office1"));
    }

    @Test
    public void test_nestedCompactsAreReturnedAsDeserialized() {
        createType("Office", "id BIGINT", "name VARCHAR");
        createType("Organization", "id BIGINT", "name VARCHAR", "office Office");
        createType("OrganizationAndLong", "id BIGINT", "l BIGINT", "organization Organization");

        createCompactMapping(client(), "test", "UserCompactType",
                "id BIGINT", "name VARCHAR", "organizationAndLong OrganizationAndLong");


        execute("INSERT INTO test VALUES (1, 1, 'user1', (1, 1, (1, 'organization1', (1, 'office1'))))");

        SqlResult result = execute("SELECT (organizationAndLong).organization.office FROM test");
        ArrayList<SqlRow> rows = new ArrayList<>();
        for (SqlRow row : result) {
            rows.add(row);
        }
        // Even if we do lazy deserialization the result should be deserialized:
        assertEquals(1, rows.size());
        assertInstanceOf(DeserializedGenericRecord.class, rows.get(0).getObject(0));
    }

    @Test
    public void test_emptyColumnList() {
        createType("Office");
        assertThatThrownBy(() -> createCompactMapping(client(), "test", "OfficesCompactType", "office Office"))
                .hasMessageContaining("Column list is required to create Compact-based types");
    }
}
