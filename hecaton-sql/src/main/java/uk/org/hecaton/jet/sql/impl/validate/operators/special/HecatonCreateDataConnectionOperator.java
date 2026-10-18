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
import uk.org.hecaton.jet.sql.impl.validate.operators.common.HecatonSpecialOperator;
import org.apache.calcite.sql.SqlKind;

/**
 * Hecaton implementation of CREATE DATA CONNECTION operator.
 */
public class HecatonCreateDataConnectionOperator extends HecatonSpecialOperator {
    public HecatonCreateDataConnectionOperator() {
        super("CREATE DATA CONNECTION", SqlKind.OTHER_DDL);
    }

    @Override
    protected boolean checkOperandTypes(HecatonCallBinding callBinding, boolean throwOnFailure) {
        throw new UnsupportedOperationException("Never called for CREATE DATA CONNECTION.");
    }
}
