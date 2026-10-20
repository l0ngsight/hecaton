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
import uk.org.hecaton.jet.sql.impl.support.expressions.ExpressionValue.CharacterVal;
import uk.org.hecaton.jet.sql.impl.support.expressions.ExpressionValue.StringVal;
import uk.org.hecaton.sql.SqlColumnType;
import uk.org.hecaton.sql.impl.expression.ConstantExpression;
import uk.org.hecaton.sql.impl.expression.string.LowerFunction;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.sql.impl.type.QueryDataType.VARCHAR;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LowerFunctionIntegrationTest extends StringFunctionIntegrationTestSupport {
    @Override
    protected String functionName() {
        return "LOWER";
    }

    @Override
    protected SqlColumnType resultType() {
        return SqlColumnType.VARCHAR;
    }

    @Override
    protected void checkSupportedColumns() {
        checkColumn(new CharacterVal(), null);
        checkColumn(new StringVal(), null);

        checkColumn(new CharacterVal().field1('a'), "a");
        checkColumn(new CharacterVal().field1('A'), "a");

        checkColumn(new StringVal().field1(""), "");
        checkColumn(new StringVal().field1("a"), "a");
        checkColumn(new StringVal().field1("A"), "a");
        checkColumn(new StringVal().field1("abcde"), "abcde");
        checkColumn(new StringVal().field1("AbCdE"), "abcde");
        checkColumn(new StringVal().field1("ABCDE"), "abcde");
    }

    @Override
    protected void checkSupportedLiterals() {
        checkLiteral("null", null);

        checkLiteral("''", "");

        checkLiteral("'a'", "a");
        checkLiteral("'A'", "a");

        checkLiteral("'abcde'", "abcde");
        checkLiteral("'AbCdE'", "abcde");
        checkLiteral("'ABCDE'", "abcde");
    }

    @Override
    protected void checkSupportedParameters() {
        checkParameter(null, null);

        checkParameter('a', "a");
        checkParameter('A', "a");

        checkParameter("", "");
        checkParameter("a", "a");
        checkParameter("A", "a");
        checkParameter("abcde", "abcde");
        checkParameter("AbCdE", "abcde");
        checkParameter("ABCDE", "abcde");
    }

    @Test
    public void testEquals() {
        LowerFunction function = LowerFunction.create(ConstantExpression.create("1", VARCHAR));

        checkEquals(function, LowerFunction.create(ConstantExpression.create("1", VARCHAR)), true);
        checkEquals(function, LowerFunction.create(ConstantExpression.create("2", VARCHAR)), false);
    }

    @Test
    public void testSerialization() {
        LowerFunction original = LowerFunction.create(ConstantExpression.create("1", VARCHAR));
        LowerFunction restored = serializeAndCheck(original, JetSqlSerializerHook.EXPRESSION_LOWER);

        checkEquals(original, restored, true);
    }
}
