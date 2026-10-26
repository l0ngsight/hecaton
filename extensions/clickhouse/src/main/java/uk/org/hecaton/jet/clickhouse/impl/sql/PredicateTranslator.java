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

import uk.org.hecaton.jet.clickhouse.impl.TableName;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Translates a SQL predicate to a ClickHouse condition with query parameters, for the
 * predicates whose meaning is the same in ClickHouse: comparisons of columns, literals and
 * dynamic parameters (ordering comparisons not of strings, whose order differs), {@code AND},
 * {@code OR}, {@code NOT}, {@code IS [NOT] NULL}. Literals and dynamic parameters become
 * query parameters, so no value is ever part of the SQL text.
 */
final class PredicateTranslator {

    private final List<String> columns;
    private final int firstParameter;
    private final List<PushedParameter> parameters = new ArrayList<>();

    /**
     * @param columns        the ClickHouse columns of the table's fields, by field index
     * @param firstParameter the number of the first parameter, {@code p<number>}
     */
    private PredicateTranslator(List<String> columns, int firstParameter) {
        this.columns = columns;
        this.firstParameter = firstParameter;
    }

    /**
     * Returns whether the expression can be translated.
     */
    static boolean supports(RexNode node) {
        return new PredicateTranslator(null, 0).translate(node) != null;
    }

    /**
     * Translates a predicate, naming its parameters {@code p<firstParameter>},
     * {@code p<firstParameter + 1>}, ...
     *
     * @return the condition, or {@code null} if it cannot be translated
     */
    @Nullable
    static Condition translate(RexNode node, List<String> columns, int firstParameter) {
        PredicateTranslator translator = new PredicateTranslator(columns, firstParameter);
        String sql = translator.translate(node);
        return sql == null ? null : new Condition(sql, translator.parameters);
    }

    /**
     * A condition in ClickHouse SQL and its parameters.
     */
    record Condition(String sql, List<PushedParameter> parameters) {
    }

    @Nullable
    private String translate(RexNode node) {
        if (node instanceof RexInputRef ref) {
            return columns == null ? "x" : TableName.quote(columns.get(ref.getIndex()));
        } else if (node instanceof RexLiteral literal) {
            return literal(literal);
        } else if (node instanceof RexDynamicParam param) {
            return dynamicParameter(param);
        } else if (node instanceof RexCall call) {
            return call(call);
        }
        return null;
    }

    @Nullable
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    private String call(RexCall call) {
        SqlKind kind = call.getKind();
        List<RexNode> operands = call.getOperands();
        switch (kind) {
            case AND, OR -> {
                List<String> parts = new ArrayList<>();
                for (RexNode operand : operands) {
                    String part = translate(operand);
                    if (part == null) {
                        return null;
                    }
                    parts.add(part);
                }
                return "(" + String.join(kind == SqlKind.AND ? " AND " : " OR ", parts) + ")";
            }
            case NOT -> {
                String operand = translate(operands.get(0));
                return operand == null ? null : "(NOT " + operand + ")";
            }
            case IS_NULL, IS_NOT_NULL -> {
                String operand = translate(operands.get(0));
                return operand == null ? null : "(" + operand + (kind == SqlKind.IS_NULL ? " IS NULL)" : " IS NOT NULL)");
            }
            case EQUALS, NOT_EQUALS, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL -> {
                boolean ordering = kind != SqlKind.EQUALS && kind != SqlKind.NOT_EQUALS;
                if (ordering && (isString(operands.get(0)) || isString(operands.get(1)))) {
                    // ClickHouse orders strings by their UTF-8 bytes, Hecaton by UTF-16 code units
                    return null;
                }
                String left = translate(operands.get(0));
                String right = translate(operands.get(1));
                if (left == null || right == null) {
                    return null;
                }
                return "(" + left + " " + operator(kind) + " " + right + ")";
            }
            default -> {
                return null;
            }
        }
    }

    private static boolean isString(RexNode node) {
        return node.getType().getSqlTypeName().getFamily() == SqlTypeFamily.CHARACTER;
    }

    private static String operator(SqlKind kind) {
        return switch (kind) {
            case EQUALS -> "=";
            case NOT_EQUALS -> "!=";
            case LESS_THAN -> "<";
            case LESS_THAN_OR_EQUAL -> "<=";
            case GREATER_THAN -> ">";
            case GREATER_THAN_OR_EQUAL -> ">=";
            default -> throw new IllegalArgumentException(kind.toString());
        };
    }

    @Nullable
    private String literal(RexLiteral literal) {
        SqlTypeName type = literal.getType().getSqlTypeName();
        PushedParameter.Kind kind = PushedParameter.kind(type);
        if (kind == null) {
            return null;
        }
        if (literal.isNull()) {
            return "NULL";
        }
        Object value = switch (kind) {
            case BOOLEAN -> literal.getValueAs(Boolean.class);
            case STRING -> literal.getValueAs(String.class);
            default -> literal.getValueAs(BigDecimal.class);
        };
        return parameter(PushedParameter.constant(kind, literal.getType().getScale(), value));
    }

    @Nullable
    private String dynamicParameter(RexDynamicParam param) {
        PushedParameter.Kind kind = PushedParameter.kind(param.getType().getSqlTypeName());
        if (kind == null || kind == PushedParameter.Kind.DECIMAL) {
            // the scale of a decimal argument is not known before the query runs
            return null;
        }
        return parameter(PushedParameter.dynamic(kind, param.getType().getScale(), param.getIndex()));
    }

    private String parameter(PushedParameter parameter) {
        String name = "p" + (firstParameter + parameters.size());
        parameters.add(parameter);
        return "{" + name + ":" + parameter.clickHouseType() + "}";
    }
}
