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
import uk.org.hecaton.jet.sql.impl.validate.operators.common.HecatonBinaryOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.typeinference.BinaryOperatorOperandTypeInference;
import uk.org.hecaton.jet.sql.impl.validate.types.HecatonTypeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlMonotonicity;

public final class HecatonArithmeticOperator extends HecatonBinaryOperator {

    public static final HecatonArithmeticOperator PLUS = new HecatonArithmeticOperator(SqlStdOperatorTable.PLUS);
    public static final HecatonArithmeticOperator MINUS = new HecatonArithmeticOperator(SqlStdOperatorTable.MINUS);
    public static final HecatonArithmeticOperator MULTIPLY = new HecatonArithmeticOperator(SqlStdOperatorTable.MULTIPLY);
    public static final HecatonArithmeticOperator DIVIDE = new HecatonArithmeticOperator(SqlStdOperatorTable.DIVIDE);
    public static final HecatonArithmeticOperator REMAINDER =
            new HecatonArithmeticOperator(SqlStdOperatorTable.PERCENT_REMAINDER);

    private HecatonArithmeticOperator(SqlBinaryOperator base) {
        super(
                base.getName(),
                base.getKind(),
                base.getLeftPrec(),
                true,
                new ArithmeticTypeInference(),
                BinaryOperatorOperandTypeInference.INSTANCE
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.BINARY;
    }

    @Override
    public boolean checkOperandTypes(HecatonCallBinding binding, boolean throwOnFailure) {
        return HecatonArithmeticOperatorUtils.checkOperandTypes(binding, throwOnFailure, kind);
    }

    @Override
    public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        return SqlMonotonicity.NOT_MONOTONIC;
    }

    public static final class ArithmeticTypeInference implements SqlReturnTypeInference {
        @Override
        public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
            assert opBinding.getOperandCount() == 2;

            if (HecatonTypeUtils.isIntervalType(opBinding.getOperandType(0))) {
                // If interval is the first parameter, then use the second parameter as return type.
                return ReturnTypes.ARG1_NULLABLE.inferReturnType(opBinding);
            } else {
                return ReturnTypes.ARG0_NULLABLE.inferReturnType(opBinding);
            }
        }
    }
}
