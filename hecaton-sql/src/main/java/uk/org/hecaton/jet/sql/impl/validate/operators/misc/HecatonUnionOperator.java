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
import uk.org.hecaton.jet.sql.impl.validate.operators.common.HecatonSetOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.Objects;

public class HecatonUnionOperator extends HecatonSetOperator {
    public static final HecatonUnionOperator UNION = new HecatonUnionOperator("UNION", false);
    public static final HecatonUnionOperator UNION_ALL = new HecatonUnionOperator("UNION ALL", true);

    protected HecatonUnionOperator(String name, boolean all) {
        super(name,
                SqlKind.UNION,
                SqlStdOperatorTable.UNION.getLeftPrec(),
                all
        );
    }

    @Override
    protected boolean checkOperandTypes(HecatonCallBinding callBinding, boolean throwOnFailure) {
        return Objects.requireNonNull(getOperandTypeChecker()).checkOperandTypes(callBinding, throwOnFailure);
    }
}
