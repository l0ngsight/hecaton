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

package uk.org.hecaton.sql.impl.expression.string;

import uk.org.hecaton.jet.sql.impl.JetSqlSerializerHook;
import uk.org.hecaton.sql.impl.expression.Expression;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;
import uk.org.hecaton.sql.impl.expression.UniExpression;
import uk.org.hecaton.sql.impl.row.Row;
import uk.org.hecaton.sql.impl.type.QueryDataType;

public class InitcapFunction extends UniExpression<String> {
    public InitcapFunction() {
        // No-op.
    }

    private InitcapFunction(Expression<?> operand) {
        super(operand);
    }

    public static InitcapFunction create(Expression<?> operand) {
        return new InitcapFunction(operand);
    }

    @Override
    public String eval(Row row, ExpressionEvalContext context) {
        String value = StringFunctionUtils.asVarchar(operand, row, context);

        return StringFunctionUtils.initcap(value);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.VARCHAR;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.EXPRESSION_INITCAP;
    }
}
