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

package uk.org.hecaton.jet.sql.impl.validate;

import uk.org.hecaton.jet.pipeline.file.AvroFileFormat;
import uk.org.hecaton.jet.pipeline.file.CsvFileFormat;
import uk.org.hecaton.jet.pipeline.file.ParquetFileFormat;
import uk.org.hecaton.jet.sql.impl.aggregate.function.HecatonAvgAggFunction;
import uk.org.hecaton.jet.sql.impl.aggregate.function.HecatonCountAggFunction;
import uk.org.hecaton.jet.sql.impl.aggregate.function.HecatonHopTableFunction;
import uk.org.hecaton.jet.sql.impl.aggregate.function.HecatonJsonArrayAggFunction;
import uk.org.hecaton.jet.sql.impl.aggregate.function.HecatonJsonObjectAggFunction;
import uk.org.hecaton.jet.sql.impl.aggregate.function.HecatonMinMaxAggFunction;
import uk.org.hecaton.jet.sql.impl.aggregate.function.HecatonSumAggFunction;
import uk.org.hecaton.jet.sql.impl.aggregate.function.HecatonTumbleTableFunction;
import uk.org.hecaton.jet.sql.impl.aggregate.function.ImposeOrderFunction;
import uk.org.hecaton.jet.sql.impl.connector.SqlConnector;
import uk.org.hecaton.jet.sql.impl.connector.file.FileTableFunction;
import uk.org.hecaton.jet.sql.impl.connector.generator.SeriesGeneratorTableFunction;
import uk.org.hecaton.jet.sql.impl.connector.generator.StreamGeneratorTableFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.common.HecatonDescriptorOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.datetime.HecatonExtractFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.datetime.HecatonToCharFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.datetime.HecatonToEpochMillisFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.datetime.HecatonToTimestampTzFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.json.HecatonJsonArrayFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.json.HecatonJsonObjectFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.json.HecatonJsonQueryFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.json.HecatonJsonValueFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.math.HecatonAbsFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.math.HecatonDoubleBiFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.math.HecatonDoubleFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.math.HecatonFloorCeilFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.math.HecatonModFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.math.HecatonRandFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.math.HecatonRoundTruncateFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.math.HecatonSignFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.misc.HecatonArithmeticOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.misc.HecatonCaseOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.misc.HecatonCastFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.misc.HecatonCoalesceFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.misc.HecatonDescOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.misc.HecatonNullIfFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.misc.HecatonUnaryOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.misc.HecatonUnionOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.misc.HecatonWithinGroupOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.predicate.HecatonAndOrPredicate;
import uk.org.hecaton.jet.sql.impl.validate.operators.predicate.HecatonBetweenOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.predicate.HecatonComparisonPredicate;
import uk.org.hecaton.jet.sql.impl.validate.operators.predicate.HecatonExistsOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.predicate.HecatonInOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.predicate.HecatonIsTrueFalseNullPredicate;
import uk.org.hecaton.jet.sql.impl.validate.operators.predicate.HecatonNotPredicate;
import uk.org.hecaton.jet.sql.impl.validate.operators.special.HecatonCollectionTableOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.special.HecatonGetDdlFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.special.HecatonMapValueConstructor;
import uk.org.hecaton.jet.sql.impl.validate.operators.special.HecatonRowOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.special.HecatonSqlCase;
import uk.org.hecaton.jet.sql.impl.validate.operators.special.HecatonValuesOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.string.HecatonConcatOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.string.HecatonConcatWSOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.string.HecatonLikeOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.string.HecatonPositionFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.string.HecatonReplaceFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.string.HecatonStringFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.string.HecatonSubstringFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.string.HecatonTrimFunction;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInfixOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.util.ArrayList;
import java.util.List;

import static uk.org.hecaton.jet.sql.impl.validate.HecatonResources.RESOURCES;

/**
 * Operator table.
 * <p>
 * All supported operators and functions must be defined in this table, even if they are already defined in the
 * {@link SqlStdOperatorTable}. This is needed to ensure that we have the full control over inference and coercion
 * strategies.
 */
@SuppressWarnings({"unused", "checkstyle:ClassDataAbstractionCoupling"})
public final class HecatonSqlOperatorTable extends ReflectiveSqlOperatorTable {

    //@formatter:off

    public static final SqlOperator CASE = HecatonCaseOperator.INSTANCE;
    public static final SqlFunction CAST = HecatonCastFunction.INSTANCE;

    //#region Boolean predicates.

    public static final SqlBinaryOperator AND = HecatonAndOrPredicate.AND;
    public static final SqlBinaryOperator OR = HecatonAndOrPredicate.OR;
    public static final SqlPrefixOperator NOT = new HecatonNotPredicate();
    public static final SqlInfixOperator BETWEEN_ASYMMETRIC = HecatonBetweenOperator.BETWEEN_ASYMMETRIC;
    public static final SqlInfixOperator NOT_BETWEEN_ASYMMETRIC = HecatonBetweenOperator.NOT_BETWEEN_ASYMMETRIC;
    public static final SqlInfixOperator BETWEEN_SYMMETRIC = HecatonBetweenOperator.BETWEEN_SYMMETRIC;
    public static final SqlInfixOperator NOT_BETWEEN_SYMMETRIC = HecatonBetweenOperator.NOT_BETWEEN_SYMMETRIC;
    public static final SqlBinaryOperator IN = HecatonInOperator.IN;
    public static final SqlBinaryOperator NOT_IN = HecatonInOperator.NOT_IN;
    public static final SqlOperator EXISTS = HecatonExistsOperator.INSTANCE;

    //#endregion

    //#region Comparison predicates.

    public static final SqlBinaryOperator EQUALS = HecatonComparisonPredicate.EQUALS;
    public static final SqlBinaryOperator NOT_EQUALS = HecatonComparisonPredicate.NOT_EQUALS;
    public static final SqlBinaryOperator GREATER_THAN = HecatonComparisonPredicate.GREATER_THAN;
    public static final SqlBinaryOperator GREATER_THAN_OR_EQUAL = HecatonComparisonPredicate.GREATER_THAN_OR_EQUAL;
    public static final SqlBinaryOperator LESS_THAN = HecatonComparisonPredicate.LESS_THAN;
    public static final SqlBinaryOperator LESS_THAN_OR_EQUAL = HecatonComparisonPredicate.LESS_THAN_OR_EQUAL;

    //#endregion

    //#region Binary and unary operators.

    public static final SqlOperator PLUS = HecatonArithmeticOperator.PLUS;
    public static final SqlOperator MINUS = HecatonArithmeticOperator.MINUS;
    public static final SqlOperator MULTIPLY = HecatonArithmeticOperator.MULTIPLY;
    public static final SqlOperator DIVIDE = HecatonArithmeticOperator.DIVIDE;
    public static final SqlOperator REMAINDER = HecatonArithmeticOperator.REMAINDER;

    public static final SqlPrefixOperator UNARY_PLUS = HecatonUnaryOperator.PLUS;
    public static final SqlPrefixOperator UNARY_MINUS = HecatonUnaryOperator.MINUS;

    //#endregion

    //#region Relational operator

    public static final SqlOperator UNION = HecatonUnionOperator.UNION;
    public static final SqlOperator UNION_ALL = HecatonUnionOperator.UNION_ALL;

    //#endregion

    //#region "IS" predicates.

    public static final SqlPostfixOperator IS_TRUE = HecatonIsTrueFalseNullPredicate.IS_TRUE;
    public static final SqlPostfixOperator IS_NOT_TRUE = HecatonIsTrueFalseNullPredicate.IS_NOT_TRUE;
    public static final SqlPostfixOperator IS_FALSE = HecatonIsTrueFalseNullPredicate.IS_FALSE;
    public static final SqlPostfixOperator IS_NOT_FALSE = HecatonIsTrueFalseNullPredicate.IS_NOT_FALSE;
    public static final SqlPostfixOperator IS_NULL = HecatonIsTrueFalseNullPredicate.IS_NULL;
    public static final SqlPostfixOperator IS_NOT_NULL = HecatonIsTrueFalseNullPredicate.IS_NOT_NULL;

    //#endregion

    //#region Math functions.

    public static final SqlFunction ABS = HecatonAbsFunction.INSTANCE;

    public static final SqlFunction SIGN = HecatonSignFunction.INSTANCE;
    public static final SqlFunction RAND = HecatonRandFunction.INSTANCE;

    public static final SqlFunction POWER = new HecatonDoubleBiFunction("POWER");
    public static final SqlFunction SQUARE = new HecatonDoubleFunction("SQUARE");
    public static final SqlFunction SQRT = new HecatonDoubleFunction("SQRT");
    public static final SqlFunction CBRT = new HecatonDoubleFunction("CBRT");

    public static final SqlFunction COS = new HecatonDoubleFunction("COS");
    public static final SqlFunction SIN = new HecatonDoubleFunction("SIN");
    public static final SqlFunction TAN = new HecatonDoubleFunction("TAN");
    public static final SqlFunction COT = new HecatonDoubleFunction("COT");
    public static final SqlFunction ACOS = new HecatonDoubleFunction("ACOS");
    public static final SqlFunction ASIN = new HecatonDoubleFunction("ASIN");
    public static final SqlFunction ATAN = new HecatonDoubleFunction("ATAN");
    public static final SqlFunction ATAN2 = new HecatonDoubleBiFunction("ATAN2");
    public static final SqlFunction EXP = new HecatonDoubleFunction("EXP");
    public static final SqlFunction LN = new HecatonDoubleFunction("LN");
    public static final SqlFunction LOG10 = new HecatonDoubleFunction("LOG10");
    public static final SqlFunction DEGREES = new HecatonDoubleFunction("DEGREES");
    public static final SqlFunction RADIANS = new HecatonDoubleFunction("RADIANS");

    public static final SqlFunction FLOOR = HecatonFloorCeilFunction.FLOOR;
    public static final SqlFunction CEIL = HecatonFloorCeilFunction.CEIL;

    public static final SqlFunction MOD = HecatonModFunction.MOD;

    public static final SqlFunction ROUND = HecatonRoundTruncateFunction.ROUND;
    public static final SqlFunction TRUNCATE = HecatonRoundTruncateFunction.TRUNCATE;

    //#endregion

    //#region String functions.

    public static final SqlBinaryOperator CONCAT = HecatonConcatOperator.INSTANCE;
    public static final SqlFunction CONCAT_WS = HecatonConcatWSOperator.INSTANCE;

    public static final SqlSpecialOperator LIKE = HecatonLikeOperator.LIKE;
    public static final SqlSpecialOperator NOT_LIKE = HecatonLikeOperator.NOT_LIKE;

    public static final SqlFunction SUBSTRING = HecatonSubstringFunction.INSTANCE;

    public static final SqlFunction TRIM = HecatonTrimFunction.INSTANCE;

    public static final SqlFunction RTRIM = HecatonStringFunction.RTRIM;
    public static final SqlFunction LTRIM = HecatonStringFunction.LTRIM;
    public static final SqlFunction BTRIM = HecatonStringFunction.BTRIM;

    public static final SqlFunction ASCII = HecatonStringFunction.ASCII;
    public static final SqlFunction INITCAP = HecatonStringFunction.INITCAP;

    public static final SqlFunction CHAR_LENGTH = HecatonStringFunction.CHAR_LENGTH;
    public static final SqlFunction CHARACTER_LENGTH = HecatonStringFunction.CHARACTER_LENGTH;
    public static final SqlFunction LENGTH = HecatonStringFunction.LENGTH;

    public static final SqlFunction LOWER = HecatonStringFunction.LOWER;
    public static final SqlFunction UPPER = HecatonStringFunction.UPPER;

    public static final SqlFunction REPLACE = HecatonReplaceFunction.INSTANCE;
    public static final SqlFunction POSITION = HecatonPositionFunction.INSTANCE;

    public static final SqlFunction EXTRACT = HecatonExtractFunction.INSTANCE;
    public static final SqlFunction TO_TIMESTAMP_TZ = HecatonToTimestampTzFunction.INSTANCE;
    public static final SqlFunction TO_EPOCH_MILLIS = HecatonToEpochMillisFunction.INSTANCE;
    public static final SqlFunction TO_CHAR = HecatonToCharFunction.INSTANCE;

    public static final SqlFunction JSON_QUERY = HecatonJsonQueryFunction.INSTANCE;
    public static final SqlFunction JSON_VALUE = HecatonJsonValueFunction.INSTANCE;
    public static final SqlFunction JSON_OBJECT = HecatonJsonObjectFunction.INSTANCE;
    public static final SqlFunction JSON_ARRAY = HecatonJsonArrayFunction.INSTANCE;
    public static final SqlFunction JSON_ARRAYAGG_ABSENT_ON_NULL = HecatonJsonArrayAggFunction.ABSENT_ON_NULL_INSTANCE;
    public static final SqlFunction JSON_ARRAYAGG_NULL_ON_NULL = HecatonJsonArrayAggFunction.NULL_ON_NULL_INSTANCE;
    public static final SqlFunction JSON_OBJECTAGG_ABSENT_ON_NULL = HecatonJsonObjectAggFunction.ABSENT_ON_NULL_INSTANCE;
    public static final SqlFunction JSON_OBJECTAGG_NULL_ON_NULL = HecatonJsonObjectAggFunction.NULL_ON_NULL_INSTANCE;

    public static final SqlOperator WITHIN_GROUP = HecatonWithinGroupOperator.INSTANCE;
    public static final SqlOperator DOT = SqlStdOperatorTable.DOT;

    public static final SqlPostfixOperator DESC = HecatonDescOperator.DESC;

    //#endregion

    //#region null check functions

    public static final SqlFunction NULLIF = HecatonNullIfFunction.INSTANCE;
    public static final SqlFunction COALESCE = HecatonCoalesceFunction.INSTANCE;

    //#endregion

    //#region Windowing functions.

    public static final SqlOperator DESCRIPTOR = new HecatonDescriptorOperator();
    public static final SqlFunction IMPOSE_ORDER = new ImposeOrderFunction();
    public static final SqlFunction TUMBLE = new HecatonTumbleTableFunction();
    public static final SqlFunction HOP = new HecatonHopTableFunction();

    //#endregion

    //#region Aggregation functions.

    public static final SqlFunction COUNT = new HecatonCountAggFunction();
    public static final SqlFunction SUM = new HecatonSumAggFunction();
    public static final SqlFunction AVG = new HecatonAvgAggFunction();
    public static final SqlFunction MIN = new HecatonMinMaxAggFunction(SqlKind.MIN);
    public static final SqlFunction MAX = new HecatonMinMaxAggFunction(SqlKind.MAX);

    //#endregion

    //#region Generator functions.

    public static final SqlFunction GENERATE_SERIES = new SeriesGeneratorTableFunction();
    public static final SqlFunction GENERATE_STREAM = new StreamGeneratorTableFunction();

    //#endregion

    //#region File table functions.

    public static final SqlFunction CSV_FILE = new FileTableFunction("CSV_FILE", CsvFileFormat.FORMAT_CSV);
    public static final SqlFunction JSON_FLAT_FILE = new FileTableFunction("JSON_FLAT_FILE", SqlConnector.JSON_FLAT_FORMAT);
    public static final SqlFunction AVRO_FILE = new FileTableFunction("AVRO_FILE", AvroFileFormat.FORMAT_AVRO);
    public static final SqlFunction PARQUET_FILE = new FileTableFunction("PARQUET_FILE", ParquetFileFormat.FORMAT_PARQUET);

    //#endregion

    //#region Miscellaneous.

    public static final SqlSpecialOperator VALUES = new HecatonValuesOperator();
    public static final SqlSpecialOperator ROW = new HecatonRowOperator();
    public static final SqlSpecialOperator COLLECTION_TABLE = new HecatonCollectionTableOperator("TABLE");
    public static final SqlSpecialOperator MAP_VALUE_CONSTRUCTOR = new HecatonMapValueConstructor();

    //#region System

    public static final SqlFunction GET_DDL = HecatonGetDdlFunction.INSTANCE;

    //#endregion

    // We use an operator that doesn't implement the HecatonOperandTypeCheckerAware interface.
    // The reason is that HecatonOperandTypeCheckerAware.prepareBinding() gets the operand type for
    // all operands, but in case of this operator we must not get it for the argument name operand, it's
    // an SQL identifier and Calcite tries to resolve it as a column name. The other parameter accepts
    // ANY type so there's no need for this
    public static final SqlSpecialOperator ARGUMENT_ASSIGNMENT = SqlStdOperatorTable.ARGUMENT_ASSIGNMENT;

    //#endregion

    //@formatter:on

    private static final HecatonSqlOperatorTable INSTANCE = new HecatonSqlOperatorTable();

    static {
        INSTANCE.init();
    }

    private HecatonSqlOperatorTable() {
        // No-op.
    }

    public static HecatonSqlOperatorTable instance() {
        return INSTANCE;
    }

    @Override
    public void lookupOperatorOverloads(
            SqlIdentifier name,
            SqlFunctionCategory category,
            SqlSyntax syntax,
            List<SqlOperator> operators,
            SqlNameMatcher nameMatcher
    ) {
        super.lookupOperatorOverloads(name, category, syntax, operators, SqlNameMatchers.withCaseSensitive(false));
    }

    /**
     * Visitor that rewrites Calcite operators with operators from this table.
     */
    static final class RewriteVisitor extends SqlBasicVisitor<SqlNode> {
        private final HecatonSqlValidator validator;

        RewriteVisitor(HecatonSqlValidator validator) {
            this.validator = validator;
        }

        @Override
        public SqlNode visit(SqlNodeList nodeList) {
            for (int i = 0; i < nodeList.size(); i++) {
                SqlNode node = nodeList.get(i);
                SqlNode rewritten = node.accept(this);
                if (rewritten != null && rewritten != node) {
                    nodeList.set(i, rewritten);
                }
            }
            return nodeList;
        }

        @Override
        public SqlNode visit(SqlCall call) {
            call = rewriteCall(call);
            for (int i = 0; i < call.getOperandList().size(); i++) {
                SqlNode operand = call.getOperandList().get(i);
                if (operand == null) {
                    continue;
                }
                SqlNode rewritten = operand.accept(this);
                if (rewritten != null && rewritten != operand) {
                    call.setOperand(i, rewritten);
                }
            }
            return call;
        }

        private SqlCall rewriteCall(SqlCall call) {
            if (call instanceof SqlBasicCall basicCall) {
                // An alias is declared as a SqlBasicCall with "SqlKind.AS". We do not need to rewrite aliases, so skip it.
                if (call.getKind() == SqlKind.AS) {
                    return call;
                }
                SqlOperator operator = basicCall.getOperator();

                List<SqlOperator> resolvedOperators = new ArrayList<>(1);

                validator.getOperatorTable().lookupOperatorOverloads(
                        operator.getNameAsId(),
                        null,
                        operator.getSyntax(),
                        resolvedOperators,
                        SqlNameMatchers.withCaseSensitive(false)
                );

                if (resolvedOperators.isEmpty()) {
                    throw functionDoesNotExist(call);
                }

                assert resolvedOperators.size() == 1;

                basicCall.setOperator(resolvedOperators.get(0));
            } else if (call instanceof SqlCase sqlCase) {
                return new HecatonSqlCase(sqlCase.getParserPosition(), sqlCase.getValueOperand(), sqlCase.getWhenOperands(),
                        sqlCase.getThenOperands(), sqlCase.getElseOperand());
            }
            return call;
        }

        private static SqlNodeList removeNullWithinInStatement(SqlNodeList valueList) {
            SqlNodeList list = new SqlNodeList(valueList.getParserPosition());
            for (SqlNode node : valueList.getList()) {
                if (SqlUtil.isNullLiteral(node, false)) {
                    continue;
                }
                list.add(node);
            }
            return list;
        }

        private CalciteException functionDoesNotExist(SqlCall call) {
            throw RESOURCES.functionDoesNotExist(call.getOperator().getName()).ex();
        }
    }
}
