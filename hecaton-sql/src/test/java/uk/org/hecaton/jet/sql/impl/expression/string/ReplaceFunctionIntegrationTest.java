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

package uk.org.hecaton.jet.sql.impl.expression.string;

import uk.org.hecaton.jet.sql.impl.JetSqlSerializerHook;
import uk.org.hecaton.jet.sql.impl.expression.ExpressionTestSupport;
import uk.org.hecaton.sql.HecatonSqlException;
import uk.org.hecaton.sql.SqlColumnType;
import uk.org.hecaton.sql.SqlRow;
import uk.org.hecaton.sql.impl.expression.ConstantExpression;
import uk.org.hecaton.sql.impl.expression.string.ReplaceFunction;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplaceFunctionIntegrationTest extends ExpressionTestSupport {

    @Test
    public void test() {
        put("xyz");

        check(sql("this", "'x'", "'X'"), "Xyz");
        check(sql("this", "'x'", "'XX'"), "XXyz");
        check(sql("this", "'none'", "'xxx'"), "xyz");
    }

    @Test
    public void testEmptyString() {
        put("yyy");

        check(sql("this", "''", "'Y'"), "yyy");
        check(sql("this", "'y'", "''"), "");

        put("");

        check(sql("this", "''", "'Y'"), "");
        check(sql("this", "''", "''"), "");
    }

    @Test
    public void testNull() {
        put("xyz");

        check(sql("NULL", "NULL", "NULL"), null);
        check(sql("NULL", "'x'", "'X'"), null);
        check(sql("NULL", "NULL", "'x'"), null);
        check(sql("NULL", "'x'", "NULL"), null);
        check(sql("this", "NULL", "'X'"), null);
        check(sql("this", "'x'", "NULL"), null);
        check(sql("this", "NULL", "NULL"), null);
    }

    @Test
    public void testMultipleMatch() {
        put("xyzx");

        check(sql("this", "'x'", "'X'"), "XyzX");
        check(sql("this", "'x'", "''"), "yz");
    }

    @Test
    public void testFailure() {
        put(123);

        checkFail(sql("this", "123", "333"));
        checkFail(sql("this", "123", "'333'"));
        checkFail(sql("this", "'123'", "333"));

        put(true);

        checkFail(sql("this", "123", "333"));
        checkFail(sql("this", "123", "'333'"));
        checkFail(sql("this", "'123'", "333"));

        put("123");

        checkFail(sql("this", "123", "333"));
    }

    @Test
    public void testEquals() {
        ReplaceFunction f = createFunction("xyz", "x", "X");

        checkEquals(f, createFunction("xyz", "x", "X"), true);

        checkEquals(f, createFunction("xyz", "x", "Y"), false);
    }

    @Test
    public void testSerialization() {
        ReplaceFunction f = createFunction("xyz", "x", "X");
        ReplaceFunction deserialized = serializeAndCheck(f, JetSqlSerializerHook.EXPRESSION_REPLACE);

        checkEquals(f, deserialized, true);
    }

    private void check(String sql, String expectedResult) {
        List<SqlRow> rows = execute(sql);
        assertGreaterOrEquals("size must be greater than 0", rows.size(), 0);
        SqlRow row = rows.get(0);

        assertEquals(SqlColumnType.VARCHAR, row.getMetadata().getColumn(0).getType());
        assertEquals(expectedResult, row.getObject(0));
    }

    private void checkFail(String sql) {
        try {
            execute(sql);
            fail("Test should have been failed");
        } catch (Exception e) {
            assertTrue(e instanceof HecatonSqlException);
        }
    }

    private String name() {
        return "REPLACE";
    }

    private String sql(Object original, Object from, Object to) {
        return String.format("SELECT %s(%s, %s, %s) FROM map", name(), original, from, to);
    }

    private ReplaceFunction createFunction(String original, String from, String to) {
        return ReplaceFunction.create(
                ConstantExpression.create(original, QueryDataType.VARCHAR),
                ConstantExpression.create(from, QueryDataType.VARCHAR),
                ConstantExpression.create(to, QueryDataType.VARCHAR)
        );
    }

}
