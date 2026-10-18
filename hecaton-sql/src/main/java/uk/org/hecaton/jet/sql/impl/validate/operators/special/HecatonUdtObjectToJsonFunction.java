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

package uk.org.hecaton.jet.sql.impl.validate.operators.special;

import uk.org.hecaton.jet.sql.impl.validate.HecatonCallBinding;
import uk.org.hecaton.jet.sql.impl.validate.operators.common.HecatonFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import uk.org.hecaton.jet.sql.impl.validate.types.HecatonJsonType;
import uk.org.hecaton.jet.sql.impl.validate.types.HecatonObjectType;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;

public class HecatonUdtObjectToJsonFunction extends HecatonFunction {
    public static final HecatonFunction INSTANCE = new HecatonUdtObjectToJsonFunction();

    public HecatonUdtObjectToJsonFunction() {
        super(
                "UDT_OBJECT_TO_JSON",
                SqlKind.OTHER_FUNCTION,
                opBinding -> HecatonJsonType.create(opBinding.getOperandType(0).isNullable()),
                new ReplaceUnknownOperandTypeInference(SqlTypeName.ANY),
                SqlFunctionCategory.SYSTEM
        );
    }

    @Override
    protected boolean checkOperandTypes(final HecatonCallBinding callBinding, final boolean throwOnFailure) {
        return callBinding.getOperandType(0) instanceof HecatonObjectType;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
    }
}
