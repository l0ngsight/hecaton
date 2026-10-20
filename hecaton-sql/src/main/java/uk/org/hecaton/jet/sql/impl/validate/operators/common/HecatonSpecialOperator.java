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

package uk.org.hecaton.jet.sql.impl.validate.operators.common;

import uk.org.hecaton.jet.sql.impl.validate.HecatonCallBinding;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import static uk.org.hecaton.jet.sql.impl.validate.operators.typeinference.HecatonReturnTypeInference.wrap;

/**
 * A common subclass for special operators.
 * <p>
 * See {@link HecatonOperandTypeCheckerAware} for motivation.
 */
public abstract class HecatonSpecialOperator extends SqlSpecialOperator implements HecatonOperandTypeCheckerAware {

    protected HecatonSpecialOperator(
            String name,
            SqlKind kind,
            int prec,
            boolean leftAssoc,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference
    ) {
        super(name, kind, prec, leftAssoc, wrap(returnTypeInference), operandTypeInference, null);
    }

    protected HecatonSpecialOperator(String name, SqlKind kind) {
        super(name, kind);
    }

    @Override
    public final boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        HecatonCallBinding bindingOverride = prepareBinding(callBinding);

        return checkOperandTypes(bindingOverride, throwOnFailure);
    }

    protected abstract boolean checkOperandTypes(HecatonCallBinding callBinding, boolean throwOnFailure);
}
