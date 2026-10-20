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

package uk.org.hecaton.jet.sql.impl.validate;

import uk.org.hecaton.internal.util.BiTuple;
import uk.org.hecaton.jet.sql.impl.schema.HecatonTableFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.common.HecatonFunction;
import uk.org.hecaton.jet.sql.impl.validate.operators.common.HecatonOperandTypeCheckerAware;
import uk.org.hecaton.jet.sql.impl.validate.operators.misc.HecatonCaseOperator;
import uk.org.hecaton.jet.sql.impl.validate.operators.typeinference.HecatonReturnTypeInference;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HecatonSqlOperatorTableTest {
    /**
     * Make sure there are no overrides for operators defined in the operator table.
     */
    @Test
    public void testNoOverride() {
        Map<BiTuple<String, SqlSyntax>, SqlOperator> map = new HashMap<>();

        for (SqlOperator operator : HecatonSqlOperatorTable.instance().getOperatorList()) {
            BiTuple<String, SqlSyntax> key = BiTuple.of(operator.getName(), operator.getSyntax());

            SqlOperator oldOperator = map.put(key, operator);

            assertNull("Duplicate operator \"" + operator.getName(), oldOperator);
        }
    }

    /**
     * Make sure that all our operators either define the top-level operand checker that overrides that call binding,
     * or confirm explicitly that they override the binding manually.
     */
    @Test
    public void testOperandTypeChecker() {
        for (SqlOperator operator : HecatonSqlOperatorTable.instance().getOperatorList()) {
            boolean valid = operator instanceof HecatonOperandTypeCheckerAware
                    || operator instanceof HecatonTableFunction
                    || operator instanceof HecatonCaseOperator
                    || operator == HecatonSqlOperatorTable.ARGUMENT_ASSIGNMENT
                    || operator == HecatonSqlOperatorTable.DOT;

            assertTrue("Operator must implement one of classes from " + HecatonFunction.class.getPackage().toString()
                    + ": " + operator.getClass().getSimpleName(), valid);
        }
    }

    @Test
    public void testReturnTypeInference() {
        for (SqlOperator operator : HecatonSqlOperatorTable.instance().getOperatorList()) {
            if (operator instanceof HecatonTableFunction
                    || operator == HecatonSqlOperatorTable.IN
                    || operator == HecatonSqlOperatorTable.NOT_IN
                    || operator == HecatonSqlOperatorTable.UNION
                    || operator == HecatonSqlOperatorTable.UNION_ALL
                    || operator == HecatonSqlOperatorTable.ARGUMENT_ASSIGNMENT
                    || operator == HecatonSqlOperatorTable.DOT) {
                continue;
            }
            boolean valid = operator.getReturnTypeInference() instanceof HecatonReturnTypeInference;

            assertTrue("Operator must have " + HecatonReturnTypeInference.class.getSimpleName() + ": " + operator.getClass().getSimpleName(), valid);
        }
    }
}
