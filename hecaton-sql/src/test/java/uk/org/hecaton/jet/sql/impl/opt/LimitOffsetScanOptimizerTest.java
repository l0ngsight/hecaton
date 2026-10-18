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

package uk.org.hecaton.jet.sql.impl.opt;

import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.jet.sql.impl.opt.physical.FullScanPhysicalRel;
import uk.org.hecaton.jet.sql.impl.opt.physical.IndexScanMapPhysicalRel;
import uk.org.hecaton.jet.sql.impl.opt.physical.LimitPhysicalRel;
import uk.org.hecaton.jet.sql.impl.opt.physical.SortPhysicalRel;
import uk.org.hecaton.jet.sql.impl.schema.HecatonTable;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.sql.impl.extract.QueryPath;
import uk.org.hecaton.sql.impl.schema.TableField;
import uk.org.hecaton.sql.impl.schema.map.MapTableField;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static uk.org.hecaton.sql.impl.schema.map.MapTableUtils.getPartitionedMapIndexes;
import static java.util.Arrays.asList;

public class LimitOffsetScanOptimizerTest extends OptimizerTestSupport {
    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    // Test for https://github.com/hazelcast/hazelcast/issues/19223
    @Test
    public void test_LimitAndIndexScan_arePresentInPlan() {
        String mapName = randomName();
        IMap<Integer, String> map = instance().getMap(mapName);
        map.addIndex(IndexType.SORTED, "this");
        for (int i = 0; i < 100; i++) {
            map.put(i, String.valueOf(i));
        }

        List<QueryDataType> parameterTypes = asList(QueryDataType.INT, QueryDataType.VARCHAR);
        List<TableField> mapTableFields = asList(
                new MapTableField("__key", QueryDataType.INT, false, QueryPath.KEY_PATH),
                new MapTableField("this", QueryDataType.VARCHAR, false, QueryPath.VALUE_PATH)
        );
        HecatonTable table = partitionedTable(
                mapName,
                mapTableFields,
                getPartitionedMapIndexes(mapContainer(map), mapTableFields),
                map.size()
        );

        final String sql = "SELECT * FROM " + mapName + " ORDER BY this DESC LIMIT 5 OFFSET 3";

        assertPlan(optimizePhysical(sql, parameterTypes, table).getPhysical(),
                plan(
                        planRow(0, LimitPhysicalRel.class),
                        planRow(1, IndexScanMapPhysicalRel.class)
                )
        );
    }

    @Test
    public void test_Limit_Sort_Scan_arePresentInPlan() {
        String mapName = randomName();
        IMap<Integer, String> map = instance().getMap(mapName);
        for (int i = 0; i < 100; i++) {
            map.put(i, String.valueOf(i));
        }

        List<QueryDataType> parameterTypes = asList(QueryDataType.INT, QueryDataType.VARCHAR);
        List<TableField> mapTableFields = asList(
                new MapTableField("__key", QueryDataType.INT, false, QueryPath.KEY_PATH),
                new MapTableField("this", QueryDataType.VARCHAR, false, QueryPath.VALUE_PATH)
        );
        HecatonTable table = partitionedTable(
                mapName,
                mapTableFields,
                getPartitionedMapIndexes(mapContainer(map), mapTableFields),
                map.size()
        );

        final String sql = "SELECT * FROM " + mapName + " ORDER BY this DESC LIMIT 5 OFFSET 3";

        assertPlan(optimizePhysical(sql, parameterTypes, table).getPhysical(),
                plan(
                        planRow(0, LimitPhysicalRel.class),
                        planRow(1, SortPhysicalRel.class),
                        planRow(2, FullScanPhysicalRel.class)
                )
        );
    }
}
