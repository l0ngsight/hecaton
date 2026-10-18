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

package uk.org.hecaton.jet.sql.impl.validate.operators.misc;

import uk.org.hecaton.jet.sql.impl.validate.HecatonCallBinding;
import uk.org.hecaton.jet.sql.impl.validate.operand.TypedOperandChecker;
import uk.org.hecaton.jet.sql.impl.validate.operators.common.HecatonPrefixOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import uk.org.hecaton.jet.sql.impl.validate.types.HecatonIntegerType;
import uk.org.hecaton.jet.sql.impl.validate.types.HecatonTypeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;

public final class HecatonUnaryOperator extends HecatonPrefixOperator {

    public static final HecatonUnaryOperator PLUS = new HecatonUnaryOperator(SqlStdOperatorTable.UNARY_PLUS, false);
    public static final HecatonUnaryOperator MINUS = new HecatonUnaryOperator(SqlStdOperatorTable.UNARY_MINUS, true);

    private final boolean extend;

    private HecatonUnaryOperator(
            SqlPrefixOperator base,
            boolean extend
    ) {
        super(
                base.getName(),
                base.getKind(),
                base.getLeftPrec(),
                ReturnTypes.ARG0,
                new ReplaceUnknownOperandTypeInference(BIGINT)
        );

        this.extend = extend;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
    }

    @Override
    public boolean checkOperandTypes(HecatonCallBinding binding, boolean throwOnFailure) {
        RelDataType operandType = binding.getOperandType(0);

        if (HecatonTypeUtils.isNumericIntegerType(operandType) && extend) {
            int bitWidth = ((HecatonIntegerType) operandType).getBitWidth();

            operandType = HecatonIntegerType.create(bitWidth + 1, operandType.isNullable());
        }

        TypedOperandChecker checker = TypedOperandChecker.forType(operandType);

        if (checker.isNumeric()) {
            return checker.check(binding, throwOnFailure, 0);
        }

        if (throwOnFailure) {
            throw binding.newValidationSignatureError();
        } else {
            return false;
        }
    }
}
