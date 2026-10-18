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

package uk.org.hecaton.sql.impl.type;

import uk.org.hecaton.jet.sql.impl.JetSqlSerializerHook;
import uk.org.hecaton.sql.impl.CoreSqlTestSupport;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlDaySecondIntervalTest extends CoreSqlTestSupport {
    @Test
    public void testEquals() {
        SqlDaySecondInterval value = new SqlDaySecondInterval(1);

        checkEquals(value, new SqlDaySecondInterval(1), true);
        checkEquals(value, new SqlDaySecondInterval(2), false);
    }

    @Test
    public void testSerialization() {
        SqlDaySecondInterval original = new SqlDaySecondInterval(1);
        SqlDaySecondInterval restored = serializeAndCheck(original, JetSqlSerializerHook.INTERVAL_DAY_SECOND);

        checkEquals(original, restored, true);
    }
}
