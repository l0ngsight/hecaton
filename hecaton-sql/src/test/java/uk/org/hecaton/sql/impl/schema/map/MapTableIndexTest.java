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

package uk.org.hecaton.sql.impl.schema.map;

import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.sql.impl.CoreSqlTestSupport;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static uk.org.hecaton.config.IndexType.HASH;
import static uk.org.hecaton.config.IndexType.SORTED;
import static uk.org.hecaton.sql.impl.type.QueryDataType.BIGINT;
import static uk.org.hecaton.sql.impl.type.QueryDataType.INT;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapTableIndexTest extends CoreSqlTestSupport {
    @Test
    public void testContent() {
        MapTableIndex index = new MapTableIndex("index", SORTED, 1, singletonList(1), singletonList(INT));

        assertEquals("index", index.getName());
        assertEquals(SORTED, index.getType());
        assertEquals(1, index.getComponentsCount());
        assertEquals(singletonList(1), index.getFieldOrdinals());
        assertEquals(singletonList(INT), index.getFieldConverterTypes());
    }

    @Test
    public void testEquals() {
        String name1 = "index1";
        String name2 = "index2";

        IndexType type1 = SORTED;
        IndexType type2 = HASH;

        int components1 = 1;
        int components2 = 2;

        List<Integer> ordinals1 = singletonList(1);
        List<Integer> ordinals2 = singletonList(2);

        List<QueryDataType> types1 = singletonList(INT);
        List<QueryDataType> types2 = singletonList(BIGINT);

        MapTableIndex index = new MapTableIndex(name1, type1, components1, ordinals1, types1);

        checkEquals(index, new MapTableIndex(name1, type1, components1, ordinals1, types1), true);

        checkEquals(index, new MapTableIndex(name2, type1, components1, ordinals1, types1), false);
        checkEquals(index, new MapTableIndex(name1, type2, components1, ordinals1, types1), false);
        checkEquals(index, new MapTableIndex(name1, type1, components2, ordinals1, types1), false);
        checkEquals(index, new MapTableIndex(name1, type1, components1, ordinals2, types1), false);
        checkEquals(index, new MapTableIndex(name1, type1, components1, ordinals1, types2), false);
    }
}
