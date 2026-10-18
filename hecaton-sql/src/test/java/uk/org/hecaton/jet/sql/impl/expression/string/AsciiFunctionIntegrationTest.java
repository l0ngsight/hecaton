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
import uk.org.hecaton.sql.impl.expression.string.AsciiFunction;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.sql.impl.type.QueryDataType.VARCHAR;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AsciiFunctionIntegrationTest extends StringFunctionIntegrationTestSupport {
    @Override
    protected String functionName() {
        return "ASCII";
    }

    @Override
    protected SqlColumnType resultType() {
        return SqlColumnType.INTEGER;
    }

    @Override
    protected void checkSupportedColumns() {
        checkColumn(new CharacterVal(), null);
        checkColumn(new StringVal(), null);

        checkColumn(new CharacterVal().field1('a'), codePoint('a'));
        checkColumn(new CharacterVal().field1('A'), codePoint('A'));

        checkColumn(new StringVal().field1("abc"), codePoint('a'));
        checkColumn(new StringVal().field1("ABC"), codePoint('A'));
    }

    @Override
    protected void checkSupportedLiterals() {
        checkLiteral("null", codePoint(null));

        checkLiteral("'a'", codePoint('a'));
        checkLiteral("'A'", codePoint('A'));
        checkLiteral("'abc'", codePoint('a'));
        checkLiteral("'ABC'", codePoint('A'));
    }

    @Override
    protected void checkSupportedParameters() {
        checkParameter(null, null);

        checkParameter("", 0);
        checkParameter('a', codePoint('a'));
        checkParameter('A', codePoint('A'));

        checkParameter("a", codePoint('a'));
        checkParameter("A", codePoint('A'));
        checkParameter("abc", codePoint('a'));
        checkParameter("ABC", codePoint('A'));
    }

    @Test
    public void testEquals() {
        AsciiFunction function = AsciiFunction.create(ConstantExpression.create("1", VARCHAR));

        checkEquals(function, AsciiFunction.create(ConstantExpression.create("1", VARCHAR)), true);
        checkEquals(function, AsciiFunction.create(ConstantExpression.create("2", VARCHAR)), false);
    }

    @Test
    public void testSerialization() {
        AsciiFunction original = AsciiFunction.create(ConstantExpression.create("1", VARCHAR));
        AsciiFunction restored = serializeAndCheck(original, JetSqlSerializerHook.EXPRESSION_ASCII);

        checkEquals(original, restored, true);
    }

    private static Integer codePoint(Character value) {
        if (value == null) {
            return null;
        }

        return value.toString().codePoints().toArray()[0];
    }
}
