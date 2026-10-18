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

package uk.org.hecaton.jet.sql.impl.connector.map;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.sql.SqlTestSupport;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HecatonSerialClassRunner.class)
public class SqlSelectTest extends SqlTestSupport {
    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
    }

    private static List<Row> fillIMapAndGetData(IMap<Integer, String> map, final int count) {
        assert count >= 0;
        List<Row> rows = new ArrayList<>(count);
        int i;
        for (i = 0; i < count; ++i) {
            char ch = (char) (65 + i);
            String s = String.valueOf(ch);
            map.put(i, s);
            rows.add(new Row(i, s));
        }
        assertEquals(i, count);
        return rows;
    }

    @Test
    public void test_basicSelect() {
        HecatonInstance hecatonInstance = instance();
        String name = randomName();
        createMapping(name, int.class, String.class);
        IMap<Integer, String> map = hecatonInstance.getMap(name);

        List<Row> rows = fillIMapAndGetData(map, 20);

        assertRowsAnyOrder("SELECT * FROM " + name, rows);
    }

    @Test
    public void test_selectWithEqFilter() {
        HecatonInstance hecatonInstance = instance();
        String name = randomName();
        createMapping(name, int.class, String.class);
        IMap<Integer, String> map = hecatonInstance.getMap(name);

        fillIMapAndGetData(map, 14);
        List<Row> filteredRows = singletonList(new Row(5, "F"));

        assertRowsAnyOrder("SELECT * FROM " + name + " AS I WHERE I.__key = 5", filteredRows);
    }

    @Test
    public void test_selectWithEqFilterAndProject() {
        HecatonInstance hecatonInstance = instance();
        String name = randomName();
        createMapping(name, int.class, String.class);
        IMap<Integer, String> map = hecatonInstance.getMap(name);

        fillIMapAndGetData(map, 14);
        List<Row> filteredAndProjectedRows = singletonList(new Row(10L, "F"));

        assertRowsAnyOrder("SELECT __key * 2, this FROM " + name + " AS I WHERE I.__key = 5", filteredAndProjectedRows);
    }

    @Test
    public void test_selectWithEvenNumbersFilter() {
        HecatonInstance hecatonInstance = instance();
        String name = randomName();
        createMapping(name, int.class, String.class);
        IMap<Integer, String> map = hecatonInstance.getMap(name);

        List<Row> rows = fillIMapAndGetData(map, 14);
        List<Row> filteredRows = rows.stream()
                .filter(row -> ((int) row.getValues()[0] % 2 == 0))
                .collect(toList());

        assertRowsAnyOrder("SELECT * FROM " + name + " WHERE ( __key % 2 ) = 0", filteredRows);
    }

    @Test
    public void test_selectWithProjection() {
        final int thisProjection = 1;
        HecatonInstance hecatonInstance = instance();
        String name = randomName();
        createMapping(name, int.class, String.class);
        IMap<Integer, String> map = hecatonInstance.getMap(name);

        List<Row> rows = fillIMapAndGetData(map, 20);
        List<Row> projected = rows.stream()
                .map(row -> new Row(row.getValues()[thisProjection]))
                .collect(toList());

        assertRowsAnyOrder("SELECT this FROM " + name, projected);
    }

    @Test
    public void test_selectFromView() {
        String name = randomName();
        createMapping(name, int.class, String.class);
        IMap<Integer, String> map = instance().getMap(name);

        instance().getSql().execute("CREATE VIEW v AS SELECT * FROM " + name);

        List<Row> rows = fillIMapAndGetData(map, 20);
        assertRowsAnyOrder("SELECT * FROM v", rows);
    }
}
