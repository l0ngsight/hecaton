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

package uk.org.hecaton.jet.sql.impl.connector.generator;

import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.jet.sql.impl.schema.HecatonSpecificTableFunction;
import uk.org.hecaton.jet.sql.impl.schema.HecatonSqlOperandMetadata;
import uk.org.hecaton.jet.sql.impl.schema.HecatonTable;
import uk.org.hecaton.jet.sql.impl.schema.HecatonTableFunctionParameter;
import uk.org.hecaton.jet.sql.impl.schema.HecatonTableStatistic;
import uk.org.hecaton.jet.sql.impl.validate.HecatonCallBinding;
import uk.org.hecaton.jet.sql.impl.validate.operand.TypedOperandChecker;
import uk.org.hecaton.jet.sql.impl.validate.operators.typeinference.HecatonOperandTypeInference;
import uk.org.hecaton.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import uk.org.hecaton.sql.impl.expression.Expression;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

public final class SeriesGeneratorTableFunction extends HecatonSpecificTableFunction {

    private static final String SCHEMA_NAME_SERIES = "series";
    private static final String FUNCTION_NAME = "GENERATE_SERIES";
    private static final List<HecatonTableFunctionParameter> PARAMETERS = asList(
            new HecatonTableFunctionParameter(0, "start", SqlTypeName.INTEGER, false, TypedOperandChecker.INTEGER),
            new HecatonTableFunctionParameter(1, "stop", SqlTypeName.INTEGER, false, TypedOperandChecker.INTEGER),
            new HecatonTableFunctionParameter(2, "step", SqlTypeName.INTEGER, true, TypedOperandChecker.INTEGER)
    );

    public SeriesGeneratorTableFunction() {
        super(
                FUNCTION_NAME,
                SeriesOperandMetadata.INSTANCE,
                binding -> toTable0(emptyList()).getRowType(binding.getTypeFactory())
        );
    }

    @Override
    public HecatonTable toTable(List<Expression<?>> argumentExpressions) {
        return toTable0(argumentExpressions);
    }

    private static HecatonTable toTable0(List<Expression<?>> argumentExpressions) {
        SeriesTable table = SeriesSqlConnector.createTable(SCHEMA_NAME_SERIES, randomName(), argumentExpressions);
        return new HecatonTable(table, new HecatonTableStatistic(0));
    }

    private static String randomName() {
        return SCHEMA_NAME_SERIES + "_" + UuidUtil.newUnsecureUuidString().replace('-', '_');
    }

    private static final class SeriesOperandMetadata extends HecatonSqlOperandMetadata {

        private static final SeriesOperandMetadata INSTANCE = new SeriesOperandMetadata();

        private SeriesOperandMetadata() {
            super(
                    PARAMETERS,
                    new HecatonOperandTypeInference(PARAMETERS, new ReplaceUnknownOperandTypeInference(INTEGER))
            );
        }

        @Override
        protected boolean checkOperandTypes(HecatonCallBinding binding, boolean throwOnFailure) {
            return true;
        }
    }
}
