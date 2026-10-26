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

import uk.org.hecaton.sql.SqlResult;
import uk.org.hecaton.sql.SqlRow;
import uk.org.hecaton.sql.SqlStatement;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonSerialClassRunner.class)
@Category(NightlyTest.class)
public class ClickHouseSqlConnectorIT extends ClickHouseTestSupport {

    @BeforeClass
    public static void beforeClass() {
        initializeMembers();
    }

    private static List<List<Object>> sql(String sql, Object... arguments) {
        List<List<Object>> rows = new ArrayList<>();
        try (SqlResult result = instance().getSql().execute(new SqlStatement(sql).setParameters(Arrays.asList(arguments)))) {
            if (result.isRowSet()) {
                for (SqlRow row : result) {
                    List<Object> values = new ArrayList<>();
                    for (int i = 0; i < row.getMetadata().getColumnCount(); i++) {
                        values.add(row.getObject(i));
                    }
                    rows.add(values);
                }
            }
        }
        return rows;
    }

    private static String createPeople() {
        String table = newTableName("people");
        execute("CREATE TABLE " + table + " (id Int64, name String, age Nullable(Int32), tags Array(String)) "
                + "ENGINE = MergeTree ORDER BY id");
        execute("INSERT INTO " + table + " SELECT number, concat('name-', toString(number)), "
                + "if(number % 10 = 0, NULL, toInt32(number % 50)), ['t'] FROM numbers(1000)");
        sql("CREATE MAPPING " + table + " DATA CONNECTION " + DATA_CONNECTION);
        return table;
    }

    @Test
    public void resolvesTheColumnsOfTheTable() {
        String table = createPeople();

        // the Array column has no SQL type and is left out
        assertThat(sql("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ? "
                + "ORDER BY ordinal_position", table))
                .containsExactly(List.of("id", "BIGINT"), List.of("name", "VARCHAR"), List.of("age", "INTEGER"));
        assertThat(sql("SELECT * FROM " + table + " WHERE id = 7")).containsExactly(List.of(7L, "name-7", 7));
    }

    @Test
    public void pushesSupportedPredicatesDownAndEvaluatesTheRest() {
        String table = createPeople();

        List<List<Object>> rows = sql("SELECT name FROM " + table + " WHERE id >= ? AND id < 20 AND age IS NOT NULL "
                + "AND upper(name) LIKE 'NAME-1%' AND name > 'name-12'", 10L);

        assertThat(rows).containsExactlyInAnyOrder(List.of("name-13"), List.of("name-14"), List.of("name-15"),
                List.of("name-16"), List.of("name-17"), List.of("name-18"), List.of("name-19"));
        execute("SYSTEM FLUSH LOGS");
        // ClickHouse logs the query with the parameters' values in place
        List<List<Object>> queries = query("SELECT query FROM system.query_log WHERE type = 'QueryFinish' "
                + "AND query LIKE 'SELECT % FROM " + table + "%'");
        assertThat(queries).hasSize(1);
        String pushed = (String) queries.get(0).get(0);
        assertThat(pushed)
                .contains("WHERE")
                .contains("_CAST(10, 'Nullable(Int64)')")
                .contains("_CAST(20, 'Nullable(Int64)')")
                .contains("IS NOT NULL")
                // the string ordering and the function stay in Hecaton, the unused column is not read
                .doesNotContain("upper")
                .doesNotContain("name-12")
                .doesNotContain("tags")
                .doesNotContain("*");
    }

    @Test
    public void readsNullsAndNullArguments() {
        String table = createPeople();

        assertThat(sql("SELECT count(*) FROM " + table + " WHERE age IS NULL")).containsExactly(List.of(100L));
        assertThat(sql("SELECT count(*) FROM " + table + " WHERE age = ?", (Object) null)).containsExactly(List.of(0L));
        assertThat(sql("SELECT count(*) FROM " + table + " WHERE NOT (age = 5 OR age = 6)"))
                .containsExactly(List.of(1000L - 100 - 40));
    }

    @Test
    public void readsWithFinalAndInParallel() {
        String table = newTableName("versions");
        execute("CREATE TABLE " + table + " (id Int64, value String, version UInt64) "
                + "ENGINE = ReplacingMergeTree(version) ORDER BY id");
        execute("INSERT INTO " + table + " SELECT number, 'old', 1 FROM numbers(5000)");
        execute("INSERT INTO " + table + " SELECT number, 'new', 2 FROM numbers(5000)");
        sql("CREATE MAPPING " + table + "_all EXTERNAL NAME " + table + " DATA CONNECTION " + DATA_CONNECTION);
        sql("CREATE MAPPING " + table + "_final EXTERNAL NAME " + table + " DATA CONNECTION " + DATA_CONNECTION
                + " OPTIONS ('useFinal' = 'true', 'splitColumn' = 'id', 'preferredLocalParallelism' = '3')");

        assertThat(sql("SELECT count(*) FROM " + table + "_final")).containsExactly(List.of(5000L));
        assertThat(sql("SELECT count(DISTINCT id), min(\"value\"), max(\"value\") FROM " + table + "_final"))
                .containsExactly(List.of(5000L, "new", "new"));
        // without FINAL, until ClickHouse merges the parts, both versions are there
        assertThat((Long) sql("SELECT count(*) FROM " + table + "_all").get(0).get(0)).isBetween(5000L, 10000L);
    }

    @Test
    public void insertsAndSinks() {
        String table = newTableName("events");
        execute("CREATE TABLE " + table + " (id Int64, kind LowCardinality(String), amount Decimal(18, 2), "
                + "day Date, at DateTime64(3, 'UTC'), ok Bool, version UInt64) "
                + "ENGINE = ReplacingMergeTree(version) ORDER BY id");
        sql("CREATE MAPPING " + table + " DATA CONNECTION " + DATA_CONNECTION
                + " OPTIONS ('insertBatchSize' = '100')");

        sql("INSERT INTO " + table + " SELECT v, 'gen', CAST(v AS DECIMAL) / 4, DATE '2026-10-25', "
                + "CAST('2026-10-25T12:00:00Z' AS TIMESTAMP WITH TIME ZONE), v % 2 = 0, 1 "
                + "FROM TABLE(generate_series(1, 1000))");
        sql("SINK INTO " + table + " VALUES (?, ?, ?, ?, ?, ?, ?)", 1L, "sunk", new BigDecimal("1.25"),
                LocalDate.of(2026, 10, 26), OffsetDateTime.of(2026, 10, 26, 8, 30, 0, 0, ZoneOffset.ofHours(2)), true, 2L);

        assertThat(count("SELECT count() FROM " + table + " FINAL")).isEqualTo(1000);
        assertThat(sql("SELECT kind, amount, \"day\", \"at\", ok FROM " + table + " WHERE id = 1 AND version = 2"))
                .containsExactly(List.of("sunk", new BigDecimal("1.25"), LocalDate.of(2026, 10, 26),
                        OffsetDateTime.of(2026, 10, 26, 6, 30, 0, 0, ZoneOffset.UTC), true));
        assertThat(sql("SELECT amount FROM " + table + " WHERE id = 6")).containsExactly(List.of(new BigDecimal("1.50")));
    }

    @Test
    public void mapsFieldsToOtherNamesAndTypes() {
        String table = createPeople();
        sql("CREATE MAPPING " + table + "_renamed EXTERNAL NAME " + table + " (key BIGINT EXTERNAL NAME id, "
                + "label VARCHAR EXTERNAL NAME name, years BIGINT EXTERNAL NAME age) DATA CONNECTION " + DATA_CONNECTION);

        assertThat(sql("SELECT key, label, years FROM " + table + "_renamed WHERE key = 21"))
                .containsExactly(List.of(21L, "name-21", 21L));
    }

    @Test
    public void usesADataConnectionCreatedInSql() {
        String table = createPeople();
        sql("CREATE DATA CONNECTION sql_created TYPE ClickHouse SHARED OPTIONS ('endpoints' = '"
                + endpoint(clickhouse) + "', 'username' = '" + USER + "', 'password' = '" + PASSWORD + "')");
        sql("CREATE MAPPING " + table + "_sql EXTERNAL NAME " + table + " DATA CONNECTION sql_created");

        assertThat(sql("SELECT name FROM " + table + "_sql WHERE id = 3")).containsExactly(List.of("name-3"));
    }

    @Test
    public void refusesWhatItCannotMap() {
        String table = createPeople();

        assertThatThrownBy(() -> sql("CREATE MAPPING nowhere EXTERNAL NAME " + table + " TYPE ClickHouse"))
                .hasMessageContaining("A ClickHouse mapping needs a data connection");
        assertThatThrownBy(() -> sql("CREATE MAPPING missing EXTERNAL NAME " + table + " (nope INT) DATA CONNECTION "
                + DATA_CONNECTION)).hasMessageContaining("has no column 'nope'");
        assertThatThrownBy(() -> sql("CREATE MAPPING arrays EXTERNAL NAME " + table + " (tags VARCHAR) DATA CONNECTION "
                + DATA_CONNECTION)).hasMessageContaining("has the type Array(String), which SQL does not support");
        assertThatThrownBy(() -> sql("CREATE MAPPING wrong EXTERNAL NAME " + table + " (id DATE) DATA CONNECTION "
                + DATA_CONNECTION)).hasMessageContaining("The field 'id' has the type DATE");
        assertThatThrownBy(() -> sql("UPDATE " + table + " SET name = 'x' WHERE id = 1"))
                .hasMessageContaining("UPDATE and DELETE are not supported on ClickHouse mappings");
    }
}
