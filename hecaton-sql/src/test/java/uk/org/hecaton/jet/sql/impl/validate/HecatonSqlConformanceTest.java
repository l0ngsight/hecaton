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

import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HecatonSqlConformanceTest {
    @Test
    public void testConformance() {
        HecatonSqlConformance conformance = HecatonSqlConformance.INSTANCE;

        // ROW is not supported
        assertFalse(conformance.allowExplicitRowValueConstructor());

        // Allow "LIMIT x, y" in addition to "LIMIT x OFFSET y"
        assertTrue(conformance.isLimitStartCountAllowed());

        // Allow aliases for GROUP BY and HAVING, and ordinals for GROUP BY
        assertTrue(conformance.isGroupByAlias());
        assertTrue(conformance.isGroupByOrdinal());
        assertTrue(conformance.isHavingAlias());

        // FROM keyword is a must
        assertFalse(conformance.isFromRequired());

        // MINUS in addition to EXCEPT
        assertTrue(conformance.isMinusAllowed());

        // Allow A % B
        assertTrue(conformance.isPercentRemainderAllowed());

        // Allow FUNC in addition to FUNC()
        assertTrue(conformance.allowNiladicParentheses());

        // Allow <> in addition to !=
        assertTrue(conformance.isBangEqualAllowed());
    }
}
