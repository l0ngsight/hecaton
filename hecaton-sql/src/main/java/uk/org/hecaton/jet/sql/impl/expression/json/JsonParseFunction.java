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

package uk.org.hecaton.jet.sql.impl.expression.json;

import uk.org.hecaton.core.HecatonJsonValue;
import uk.org.hecaton.jet.sql.impl.JetSqlSerializerHook;
import uk.org.hecaton.sql.impl.expression.Expression;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;
import uk.org.hecaton.sql.impl.expression.UniExpressionWithType;
import uk.org.hecaton.sql.impl.row.Row;
import uk.org.hecaton.sql.impl.type.QueryDataType;

public class JsonParseFunction extends UniExpressionWithType<HecatonJsonValue> {

    public JsonParseFunction() { }

    private JsonParseFunction(Expression<?> operand) {
        super(operand, QueryDataType.JSON);
    }

    public static JsonParseFunction create(Expression<?> operand) {
        return new JsonParseFunction(operand);
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.JSON_PARSE;
    }

    @Override
    public HecatonJsonValue eval(final Row row, final ExpressionEvalContext context) {
        final String operand = (String) this.operand.eval(row, context);
        return new HecatonJsonValue(operand);
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.JSON;
    }
}
