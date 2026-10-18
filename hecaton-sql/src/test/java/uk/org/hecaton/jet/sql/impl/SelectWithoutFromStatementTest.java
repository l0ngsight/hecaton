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

package uk.org.hecaton.jet.sql.impl;

import uk.org.hecaton.jet.sql.SqlTestSupport;
import uk.org.hecaton.sql.HecatonSqlException;
import uk.org.hecaton.sql.SqlColumnType;
import uk.org.hecaton.sql.SqlResult;
import uk.org.hecaton.sql.SqlRow;
import uk.org.hecaton.sql.SqlService;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
public class SelectWithoutFromStatementTest extends SqlTestSupport {
    private final SqlService sqlService = instance().getSql();

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Test
    public void when_fromStatementIsNotPresent_returnConstant() {
        checkWithType("SELECT 1", SqlColumnType.TINYINT, (byte) 1);
        checkWithType("SELECT 10000", SqlColumnType.SMALLINT, (short) 10000);
        checkWithType("SELECT 2000000000", SqlColumnType.INTEGER, 2000000000);
        checkWithType("SELECT 20000000000", SqlColumnType.BIGINT, 20000000000L);
        checkWithType("SELECT 1.0", SqlColumnType.DECIMAL, new BigDecimal("1.0"));
        checkWithType("SELECT 'ABCD'", SqlColumnType.VARCHAR, "ABCD");
    }

    @Test
    public void when_castIsApplied_returnCorrectType() {
        checkWithType("SELECT CAST(1 AS BIGINT)", SqlColumnType.BIGINT, 1L);
        checkWithType("SELECT CAST(1 AS DECIMAL)", SqlColumnType.DECIMAL, new BigDecimal("1"));
        checkWithType("SELECT CAST(1.0 AS DOUBLE)", SqlColumnType.DOUBLE, 1.0d);
        checkWithType("SELECT CAST(1.0 AS REAL)", SqlColumnType.REAL, 1.0f);
    }

    @Test
    public void when_functionCallIsPassed_returnFunctionResult() {
        checkWithType("SELECT TO_TIMESTAMP_TZ(0)",
                SqlColumnType.TIMESTAMP_WITH_TIME_ZONE,
                OffsetDateTime.ofInstant(Instant.EPOCH, ZoneId.systemDefault()));
        checkWithType("SELECT TO_TIMESTAMP_TZ(1)",
                SqlColumnType.TIMESTAMP_WITH_TIME_ZONE,
                OffsetDateTime.ofInstant(Instant.EPOCH.plusSeconds(1), ZoneId.systemDefault()));
    }

    @Test
    public void when_illegalArgumentsAreProvided_fail() {
        assertThrows(HecatonSqlException.class, () -> execute("SELECT A"));
    }

    @Test
    public void when_multipleColumnsSelected_returnCorrectResult() {
        final SqlRow row = execute("SELECT 1 as A, 'B' as B");
        assertEquals(SqlColumnType.TINYINT, row.getMetadata().getColumn(0).getType());
        assertEquals(SqlColumnType.VARCHAR, row.getMetadata().getColumn(1).getType());
        assertEquals((byte) 1, (byte) row.getObject("A"));
        assertEquals("B", row.getObject("B"));
    }

    private void checkWithType(String sql, SqlColumnType type, Object expected) {
        final SqlRow row = execute(sql);
        assertEquals(1, row.getMetadata().getColumnCount());
        assertEquals(type, row.getMetadata().getColumn(0).getType());
        assertEquals(expected, row.getObject(0));
    }

    private SqlRow execute(String sql) {
        final SqlResult result = sqlService.execute(sql);
        return result.iterator().next();
    }
}
