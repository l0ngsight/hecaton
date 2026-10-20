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

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

public final class StreamGeneratorTableFunction extends HecatonSpecificTableFunction {

    private static final String SCHEMA_NAME_STREAM = "stream";
    private static final String FUNCTION_NAME = "GENERATE_STREAM";
    private static final List<HecatonTableFunctionParameter> PARAMETERS = singletonList(
            new HecatonTableFunctionParameter(0, "rate", INTEGER, false, TypedOperandChecker.INTEGER)
    );

    public StreamGeneratorTableFunction() {
        super(
                FUNCTION_NAME,
                StreamOperandMetadata.INSTANCE,
                binding -> toTable0(emptyList()).getRowType(binding.getTypeFactory())
        );
    }

    @Override
    public HecatonTable toTable(List<Expression<?>> argumentExpressions) {
        return toTable0(argumentExpressions);
    }

    private static HecatonTable toTable0(List<Expression<?>> argumentExpressions) {
        StreamTable table = StreamSqlConnector.createTable(SCHEMA_NAME_STREAM, randomName(), argumentExpressions);
        return new HecatonTable(table, new HecatonTableStatistic(Integer.MAX_VALUE));
    }

    private static String randomName() {
        return SCHEMA_NAME_STREAM + "_" + UuidUtil.newUnsecureUuidString().replace('-', '_');
    }

    private static final class StreamOperandMetadata extends HecatonSqlOperandMetadata {

        private static final StreamOperandMetadata INSTANCE = new StreamOperandMetadata();

        private StreamOperandMetadata() {
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
