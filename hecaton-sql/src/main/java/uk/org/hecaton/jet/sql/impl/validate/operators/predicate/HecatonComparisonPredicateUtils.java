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
import uk.org.hecaton.jet.sql.impl.validate.HecatonSqlValidator;
import uk.org.hecaton.jet.sql.impl.validate.ValidatorResource;
import uk.org.hecaton.jet.sql.impl.validate.param.NumericPrecedenceParameterConverter;
import uk.org.hecaton.jet.sql.impl.validate.types.HecatonTypeUtils;
import uk.org.hecaton.sql.impl.ParameterConverter;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

import static uk.org.hecaton.sql.impl.type.QueryDataTypeFamily.OBJECT;

public final class HecatonComparisonPredicateUtils {
    private HecatonComparisonPredicateUtils() {
    }

    public static boolean checkOperandTypes(HecatonCallBinding binding, boolean throwOnFailure) {
        SqlNode first = binding.operand(0);
        SqlNode second = binding.operand(1);

        HecatonSqlValidator validator = binding.getValidator();

        RelDataType firstType = validator.deriveType(binding.getScope(), first);
        RelDataType secondType = validator.deriveType(binding.getScope(), second);

        assert !firstType.equals(validator.getUnknownType());
        assert !secondType.equals(validator.getUnknownType());

        return checkOperandTypes(binding, throwOnFailure, validator, first, firstType, second, secondType);
    }

    private static boolean checkOperandTypes(
            HecatonCallBinding callBinding,
            boolean throwOnFailure,
            HecatonSqlValidator validator,
            SqlNode first,
            RelDataType firstType,
            SqlNode second,
            RelDataType secondType
    ) {
        if (firstType.getSqlTypeName() == SqlTypeName.ROW || secondType.getSqlTypeName() == SqlTypeName.ROW) {
            if (throwOnFailure) {
                throw callBinding.newError(ValidatorResource.RESOURCE.rowTypeComparisonNotSupported());
            } else {
                return false;
            }
        }

        RelDataType winningType = HecatonTypeUtils.withHigherPrecedence(firstType, secondType);

        if (winningType == firstType) {
            return checkOperandTypesWithPrecedence(
                    callBinding,
                    throwOnFailure,
                    validator,
                    first,
                    firstType,
                    second,
                    secondType,
                    1
            );
        } else {
            assert winningType == secondType;

            return checkOperandTypesWithPrecedence(
                    callBinding,
                    throwOnFailure,
                    validator,
                    second,
                    secondType,
                    first,
                    firstType,
                    0
            );
        }
    }

    private static boolean checkOperandTypesWithPrecedence(
            HecatonCallBinding callBinding,
            boolean throwOnFailure,
            HecatonSqlValidator validator,
            SqlNode high,
            RelDataType highType,
            SqlNode low,
            RelDataType lowType,
            int lowIndex
    ) {
        QueryDataType highHZType = HecatonTypeUtils.toHecatonType(highType);
        QueryDataType lowHZType = HecatonTypeUtils.toHecatonType(lowType);

        if (highHZType.getTypeFamily().isNumeric()) {
            // Set flexible parameter converter that allows TINYINT/SMALLINT/INTEGER -> BIGINT conversions
            setNumericParameterConverter(validator, high, highHZType);
            setNumericParameterConverter(validator, low, highHZType);
        }

        boolean valid = validator
                .getTypeCoercion()
                .rowTypeElementCoercion(
                        callBinding.getScope(),
                        low,
                        highType,
                        lowOperandNode -> callBinding.getCall().setOperand(lowIndex, lowOperandNode));

        if (valid && highHZType.getTypeFamily() == OBJECT && lowHZType.getTypeFamily() != OBJECT) {
            valid = false;
        }

        // Custom types can not be converted to each other.
        if (highHZType.isCustomType() && lowHZType.isCustomType()) {
            if (!highHZType.getObjectTypeName().equals(lowHZType.getObjectTypeName())) {
                valid = false;
            }
            assert highHZType.getObjectTypeKind() == lowHZType.getObjectTypeKind();
        }

        // Types cannot be converted to each other.
        if (!valid && throwOnFailure) {
            throw callBinding.newValidationSignatureError();
        }

        return valid;
    }

    private static void setNumericParameterConverter(HecatonSqlValidator validator, SqlNode node, QueryDataType type) {
        if (node.getKind() == SqlKind.DYNAMIC_PARAM) {
            SqlDynamicParam node0 = (SqlDynamicParam) node;

            ParameterConverter converter = new NumericPrecedenceParameterConverter(
                    node0.getIndex(),
                    node.getParserPosition(),
                    type
            );

            validator.setParameterConverter(node0.getIndex(), converter);
        }
    }
}
