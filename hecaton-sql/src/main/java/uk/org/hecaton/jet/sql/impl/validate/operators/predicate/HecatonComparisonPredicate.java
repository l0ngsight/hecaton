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

package uk.org.hecaton.jet.sql.impl.validate.operators.predicate;

import uk.org.hecaton.jet.sql.impl.validate.HecatonCallBinding;
import uk.org.hecaton.jet.sql.impl.validate.operators.common.HecatonBinaryOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.typeinference.ComparisonOperatorOperandTypeInference;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

public final class HecatonComparisonPredicate extends HecatonBinaryOperator {

    public static final HecatonComparisonPredicate EQUALS = new HecatonComparisonPredicate(
            SqlStdOperatorTable.EQUALS
    );

    public static final HecatonComparisonPredicate NOT_EQUALS = new HecatonComparisonPredicate(
            SqlStdOperatorTable.NOT_EQUALS
    );

    public static final HecatonComparisonPredicate GREATER_THAN = new HecatonComparisonPredicate(
            SqlStdOperatorTable.GREATER_THAN
    );

    public static final HecatonComparisonPredicate GREATER_THAN_OR_EQUAL = new HecatonComparisonPredicate(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL
    );

    public static final HecatonComparisonPredicate LESS_THAN = new HecatonComparisonPredicate(
            SqlStdOperatorTable.LESS_THAN
    );

    public static final HecatonComparisonPredicate LESS_THAN_OR_EQUAL = new HecatonComparisonPredicate(
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL
    );

    private HecatonComparisonPredicate(SqlBinaryOperator base) {
        super(
                base.getName(),
                base.getKind(),
                base.getLeftPrec(),
                true,
                ReturnTypes.BOOLEAN_NULLABLE,
                ComparisonOperatorOperandTypeInference.INSTANCE
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }

    @Override
    public boolean checkOperandTypes(HecatonCallBinding binding, boolean throwOnFailure) {
        return HecatonComparisonPredicateUtils.checkOperandTypes(binding, throwOnFailure);
    }
}
