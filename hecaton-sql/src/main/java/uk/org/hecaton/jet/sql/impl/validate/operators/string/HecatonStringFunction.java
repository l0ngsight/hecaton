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

package uk.org.hecaton.jet.sql.impl.validate.operators.string;

import uk.org.hecaton.jet.sql.impl.validate.HecatonCallBinding;
import uk.org.hecaton.jet.sql.impl.validate.operand.TypedOperandChecker;
import uk.org.hecaton.jet.sql.impl.validate.operators.common.HecatonFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.typeinference.ReplaceUnknownOperandTypeInference;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

public final class HecatonStringFunction extends HecatonFunction {

    public static final SqlFunction ASCII = HecatonStringFunction.withIntegerReturn("ASCII");
    public static final SqlFunction INITCAP = HecatonStringFunction.withStringReturn("INITCAP");

    public static final SqlFunction CHAR_LENGTH = HecatonStringFunction.withIntegerReturn("CHAR_LENGTH");
    public static final SqlFunction CHARACTER_LENGTH = HecatonStringFunction.withIntegerReturn("CHARACTER_LENGTH");
    public static final SqlFunction LENGTH = HecatonStringFunction.withIntegerReturn("LENGTH");

    public static final SqlFunction LOWER = HecatonStringFunction.withStringReturn("LOWER");
    public static final SqlFunction UPPER = HecatonStringFunction.withStringReturn("UPPER");

    public static final SqlFunction RTRIM = HecatonStringFunction.withStringReturn("RTRIM");
    public static final SqlFunction LTRIM = HecatonStringFunction.withStringReturn("LTRIM");
    public static final SqlFunction BTRIM = HecatonStringFunction.withStringReturn("BTRIM");

    private HecatonStringFunction(String name, SqlReturnTypeInference returnTypeInference) {
        super(
                name,
                SqlKind.OTHER_FUNCTION,
                returnTypeInference,
                new ReplaceUnknownOperandTypeInference(VARCHAR),
                SqlFunctionCategory.STRING
        );
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
    }

    @Override
    public boolean checkOperandTypes(HecatonCallBinding callBinding, boolean throwOnFailure) {
        return TypedOperandChecker.VARCHAR.check(callBinding, throwOnFailure, 0);
    }

    private static HecatonStringFunction withStringReturn(String name) {
        return new HecatonStringFunction(name, ReturnTypes.ARG0_NULLABLE);
    }

    private static HecatonStringFunction withIntegerReturn(String name) {
        return new HecatonStringFunction(name, ReturnTypes.INTEGER_NULLABLE);
    }
}
