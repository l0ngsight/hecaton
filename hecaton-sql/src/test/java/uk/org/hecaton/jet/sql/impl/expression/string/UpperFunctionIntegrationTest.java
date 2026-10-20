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
import uk.org.hecaton.sql.impl.expression.string.UpperFunction;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.sql.impl.type.QueryDataType.VARCHAR;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UpperFunctionIntegrationTest extends StringFunctionIntegrationTestSupport {
    @Override
    protected String functionName() {
        return "UPPER";
    }

    @Override
    protected SqlColumnType resultType() {
        return SqlColumnType.VARCHAR;
    }

    @Override
    protected void checkSupportedColumns() {
        checkColumn(new CharacterVal(), null);
        checkColumn(new StringVal(), null);

        checkColumn(new CharacterVal().field1('a'), "A");
        checkColumn(new CharacterVal().field1('A'), "A");

        checkColumn(new StringVal().field1(""), "");
        checkColumn(new StringVal().field1("a"), "A");
        checkColumn(new StringVal().field1("A"), "A");
        checkColumn(new StringVal().field1("abcde"), "ABCDE");
        checkColumn(new StringVal().field1("AbCdE"), "ABCDE");
        checkColumn(new StringVal().field1("ABCDE"), "ABCDE");
    }

    @Override
    protected void checkSupportedLiterals() {
        checkLiteral("null", null);

        checkLiteral("''", "");

        checkLiteral("'a'", "A");
        checkLiteral("'A'", "A");

        checkLiteral("'abcde'", "ABCDE");
        checkLiteral("'AbCdE'", "ABCDE");
        checkLiteral("'ABCDE'", "ABCDE");
    }

    @Override
    protected void checkSupportedParameters() {
        checkParameter(null, null);

        checkParameter('a', "A");
        checkParameter('A', "A");

        checkParameter("", "");
        checkParameter("a", "A");
        checkParameter("A", "A");
        checkParameter("abcde", "ABCDE");
        checkParameter("AbCdE", "ABCDE");
        checkParameter("ABCDE", "ABCDE");
    }

    @Test
    public void testEquals() {
        UpperFunction function = UpperFunction.create(ConstantExpression.create("1", VARCHAR));

        checkEquals(function, UpperFunction.create(ConstantExpression.create("1", VARCHAR)), true);
        checkEquals(function, UpperFunction.create(ConstantExpression.create("2", VARCHAR)), false);
    }

    @Test
    public void testSerialization() {
        UpperFunction original = UpperFunction.create(ConstantExpression.create("1", VARCHAR));
        UpperFunction restored = serializeAndCheck(original, JetSqlSerializerHook.EXPRESSION_UPPER);

        checkEquals(original, restored, true);
    }
}
