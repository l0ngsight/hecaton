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

package uk.org.hecaton.jet.sql.impl;

import uk.org.hecaton.jet.sql.SqlTestSupport;
import uk.org.hecaton.sql.SqlColumnMetadata;
import uk.org.hecaton.sql.SqlResult;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
public class SqlNullableColumnTest extends SqlTestSupport {

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Test
    public void testSelectWithNonNullSupport() {
        createMapping("map", String.class, int.class);
        instance().getMap("map").put("key", 1);

        SqlResult result = instance().getSql().execute("SELECT __key, 1 FROM map");
        List<SqlColumnMetadata> columns = result.getRowMetadata().getColumns();

        assertEquals(2, columns.size());
        assertTrue(columns.get(0).isNullable());
        assertFalse(columns.get(1).isNullable());
    }
}
