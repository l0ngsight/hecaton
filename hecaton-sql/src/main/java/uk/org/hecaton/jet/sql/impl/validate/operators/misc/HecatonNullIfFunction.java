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
import uk.org.hecaton.jet.sql.impl.validate.operators.common.HecatonFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.predicate.HecatonComparisonPredicateUtils;
import uk.org.hecaton.jet.sql.impl.validate.operators.typeinference.BinaryOperatorOperandTypeInference;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

import static uk.org.hecaton.jet.sql.impl.validate.operators.typeinference.HecatonReturnTypeInference.wrap;

public final class HecatonNullIfFunction extends HecatonFunction {
    public static final HecatonNullIfFunction INSTANCE = new HecatonNullIfFunction();

    private HecatonNullIfFunction() {
        super(
                "NULLIF",
                SqlKind.NULLIF,
                wrap(ReturnTypes.ARG0_NULLABLE),
                BinaryOperatorOperandTypeInference.INSTANCE,
                SqlFunctionCategory.SYSTEM
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }

    @Override
    protected boolean checkOperandTypes(HecatonCallBinding callBinding, boolean throwOnFailure) {
        return HecatonComparisonPredicateUtils.checkOperandTypes(callBinding, throwOnFailure);
    }
}
