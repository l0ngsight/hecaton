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

package uk.org.hecaton.jet.sql.impl.expression.math;

import uk.org.hecaton.sql.impl.QueryException;
import uk.org.hecaton.sql.impl.expression.math.ExpressionMath;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static uk.org.hecaton.sql.impl.type.QueryDataTypeUtils.DECIMAL_MATH_CONTEXT;
import static uk.org.hecaton.test.HecatonTestSupport.assertThrows;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExpressionMathTest {

    @Test
    public void testDecimalMathContext() {
        assertEquals(QueryDataType.MAX_DECIMAL_PRECISION, DECIMAL_MATH_CONTEXT.getPrecision());
        assertEquals(RoundingMode.HALF_UP, DECIMAL_MATH_CONTEXT.getRoundingMode());

        BigDecimal r = BigDecimal.valueOf(1).divide(BigDecimal.valueOf(3), DECIMAL_MATH_CONTEXT);
        assertEquals(QueryDataType.MAX_DECIMAL_PRECISION, r.precision());
    }

    @Test
    public void testLongDivideExactly() {
        assertEquals(3L, ExpressionMath.divideExact(10L, 3L));
        assertThrows(ArithmeticException.class, () -> ExpressionMath.divideExact(10L, 0L));
        assertThrows(QueryException.class, () -> ExpressionMath.divideExact(Long.MIN_VALUE, -1L));
    }

    @Test
    public void testDoubleDivideExactly() {
        assertEquals(10.0 / 3.0, ExpressionMath.divideExact(10.0, 3.0), 0.0);
        assertThrows(ArithmeticException.class, () -> ExpressionMath.divideExact(10.0, 0.0));
    }

    @Test
    public void testFloatDivideExactly() {
        assertEquals(10.0F / 3.0F, ExpressionMath.divideExact(10.0F, 3.0F), 0.0F);
        assertThrows(ArithmeticException.class, () -> ExpressionMath.divideExact(10.0F, 0.0F));
    }
}
