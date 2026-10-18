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
import uk.org.hecaton.jet.sql.impl.validate.operators.common.HecatonPostfixOperator;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

public final class HecatonDescOperator extends HecatonPostfixOperator {

    public static final HecatonDescOperator DESC = new HecatonDescOperator(SqlStdOperatorTable.DESC);

    private HecatonDescOperator(SqlPostfixOperator base) {
        super(base.getName(),
                base.getKind(),
                base.getLeftPrec(),
                base.getReturnTypeInference(),
                base.getOperandTypeInference());
    }

    @Override
    protected boolean checkOperandTypes(HecatonCallBinding callBinding, boolean throwOnFailure) {
        return true;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.any();
    }
}
