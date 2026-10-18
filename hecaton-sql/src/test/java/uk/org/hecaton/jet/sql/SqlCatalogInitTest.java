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

package uk.org.hecaton.jet.sql;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.sql.SqlResult;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HecatonSerialClassRunner.class)
public class SqlCatalogInitTest extends SqlTestSupport {
    public static final String MAP_NAME = randomName();
    public static final Config CONFIG = smallInstanceConfig();

    // test case for https://github.com/hazelcast/hazelcast/issues/21632
    @Test
    public void test() {
        HecatonInstance instance1 = createHecatonInstance(CONFIG);
        createHecatonInstance(CONFIG);
        createMapping(instance1, MAP_NAME, Integer.class, Integer.class);

        HecatonInstance instance3 = createHecatonInstance(CONFIG);
        assertClusterSizeEventually(3, instance3);
        waitAllForSafeState(instance3);
        try (SqlResult sqlResult = instance3.getSql().execute("select * from " + MAP_NAME)) {
            assertFalse(sqlResult.iterator().hasNext());
        }
    }
}
