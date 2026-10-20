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

package uk.org.hecaton.jet.sql.impl.aggregate.function;

import uk.org.hecaton.jet.sql.impl.schema.HecatonTableFunction;
import uk.org.hecaton.jet.sql.impl.schema.HecatonTableFunctionParameter;
import uk.org.hecaton.jet.sql.impl.validate.operand.AnyOperandChecker;
import uk.org.hecaton.jet.sql.impl.validate.operand.TypedOperandChecker;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

import static java.util.Arrays.asList;

public class ImposeOrderFunction extends HecatonTableFunction {

    private static final List<HecatonTableFunctionParameter> PARAMETERS = asList(
            new HecatonTableFunctionParameter(0, "input", SqlTypeName.ROW, false, TypedOperandChecker.ROW),
            new HecatonTableFunctionParameter(1, "time_col", SqlTypeName.COLUMN_LIST, false, TypedOperandChecker.COLUMN_LIST),
            new HecatonTableFunctionParameter(2, "lag", SqlTypeName.ANY, false, AnyOperandChecker.INSTANCE)
    );

    private static final SqlReturnTypeInference RETURN_TYPE_INFERENCE = binding -> {
        SqlCallBinding callBinding = ((SqlCallBinding) binding);
        return callBinding.getValidator().getValidatedNodeType(callBinding.operand(0));
    };

    public ImposeOrderFunction() {
        super("IMPOSE_ORDER", new WindowOperandMetadata(PARAMETERS, new int[]{2}), RETURN_TYPE_INFERENCE);
    }

    @Override
    public boolean argumentMustBeScalar(int ordinal) {
        return ordinal != 0;
    }
}
