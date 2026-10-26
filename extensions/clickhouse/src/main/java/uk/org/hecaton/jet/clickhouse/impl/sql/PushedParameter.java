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

package uk.org.hecaton.jet.clickhouse.impl.sql;

import uk.org.hecaton.sql.impl.type.QueryDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

/**
 * A query parameter of a pushed-down predicate: a literal's value, or a dynamic parameter
 * whose value is known when the query runs.
 */
final class PushedParameter implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final int DECIMAL_PRECISION = 76;

    /**
     * The kinds of values that are pushed down.
     */
    enum Kind { BOOLEAN, INTEGER, FLOAT32, FLOAT64, DECIMAL, STRING }

    private final Kind kind;
    private final int scale;
    private final Object constant;
    private final int dynamicIndex;

    private PushedParameter(Kind kind, int scale, Object constant, int dynamicIndex) {
        this.kind = kind;
        this.scale = Math.max(scale, 0);
        this.constant = constant;
        this.dynamicIndex = dynamicIndex;
    }

    static PushedParameter constant(Kind kind, int scale, Object value) {
        return new PushedParameter(kind, scale, value, -1);
    }

    static PushedParameter dynamic(Kind kind, int scale, int index) {
        return new PushedParameter(kind, scale, null, index);
    }

    /**
     * Returns the kind of the parameters of a SQL type, or {@code null} if it is not pushed down.
     */
    @Nullable
    static Kind kind(SqlTypeName type) {
        return switch (type) {
            case BOOLEAN -> Kind.BOOLEAN;
            case TINYINT, SMALLINT, INTEGER, BIGINT -> Kind.INTEGER;
            case REAL -> Kind.FLOAT32;
            case FLOAT, DOUBLE -> Kind.FLOAT64;
            case DECIMAL -> Kind.DECIMAL;
            case CHAR, VARCHAR -> Kind.STRING;
            default -> null;
        };
    }

    /**
     * Returns the ClickHouse type of the parameter.
     */
    String clickHouseType() {
        String type = switch (kind) {
            case BOOLEAN -> "Bool";
            case INTEGER -> "Int64";
            case FLOAT32 -> "Float32";
            case FLOAT64 -> "Float64";
            case DECIMAL -> "Decimal(" + DECIMAL_PRECISION + ", " + scale + ")";
            case STRING -> "String";
        };
        // a dynamic parameter can be null
        return "Nullable(" + type + ")";
    }

    /**
     * Returns the value of the parameter for the arguments of the query.
     */
    Object value(List<Object> arguments) {
        Object value = dynamicIndex >= 0 ? arguments.get(dynamicIndex) : constant;
        if (value == null) {
            return null;
        }
        return switch (kind) {
            case BOOLEAN -> QueryDataType.BOOLEAN.convert(value);
            case INTEGER -> QueryDataType.BIGINT.convert(value);
            case FLOAT32 -> QueryDataType.REAL.convert(value);
            case FLOAT64 -> QueryDataType.DOUBLE.convert(value);
            // only literals, whose scale is the parameter's
            case DECIMAL -> ((BigDecimal) value).toPlainString();
            case STRING -> QueryDataType.VARCHAR.convert(value);
        };
    }
}
