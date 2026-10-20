/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.map.impl.query;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.query.SampleTestObjects;
import uk.org.hecaton.query.SampleTestObjects.Employee;
import uk.org.hecaton.query.impl.IndexCopyBehavior;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class QueryNullIndexingTest extends HecatonTestSupport {

    @Parameter(0)
    public IndexCopyBehavior copyBehavior;

    @Parameters(name = "copyBehavior: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {IndexCopyBehavior.COPY_ON_READ},
                {IndexCopyBehavior.COPY_ON_WRITE},
                {IndexCopyBehavior.NEVER},
        });
    }

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithLessPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(false, Predicates.lessThan("date", 5000000L));
        assertEquals(2, dates.size());
        assertContainsAll(dates, asList(2000000L, 4000000L));
    }

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithLessEqualPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(false, Predicates.lessEqual("date", 5000000L));
        assertEquals(2, dates.size());
        assertContainsAll(dates, asList(2000000L, 4000000L));
    }

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithGreaterPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(false, Predicates.greaterThan("date", 5000000L));
        assertEquals(3, dates.size());
        assertContainsAll(dates, asList(6000000L, 8000000L, 10000000L));
    }

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithGreaterEqualPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(false, Predicates.greaterEqual("date", 6000000L));
        assertEquals(3, dates.size());
        assertContainsAll(dates, asList(6000000L, 8000000L, 10000000L));
    }

    @Test
    public void testIndexedNullValueOnUnorderedIndexStoreWithNotEqualPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(false, Predicates.notEqual("date", 2000000L));
        assertEquals(9, dates.size());
        assertContainsAll(dates, asList(4000000L, 6000000L, 8000000L, 10000000L, null));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithLessPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(true, Predicates.lessThan("date", 5000000L));
        assertEquals(2, dates.size());
        assertContainsAll(dates, asList(2000000L, 4000000L));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithLessEqualPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(true, Predicates.lessEqual("date", 5000000L));
        assertEquals(2, dates.size());
        assertContainsAll(dates, asList(2000000L, 4000000L));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithGreaterPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(true, Predicates.greaterThan("date", 5000000L));
        assertEquals(3, dates.size());
        assertContainsAll(dates, asList(6000000L, 8000000L, 10000000L));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithGreaterEqualPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(true, Predicates.greaterEqual("date", 6000000L));
        assertEquals(3, dates.size());
        assertContainsAll(dates, asList(6000000L, 8000000L, 10000000L));
    }

    @Test
    public void testIndexedNullValueOnOrderedIndexStoreWithNotEqualPredicate() {
        List<Long> dates = queryIndexedDateFieldAsNullValue(true, Predicates.notEqual("date", 2000000L));
        assertEquals(9, dates.size());
        assertContainsAll(dates, asList(4000000L, 6000000L, 8000000L, 10000000L, null));
    }

    private List<Long> queryIndexedDateFieldAsNullValue(boolean ordered,
                                                        Predicate<Integer, SampleTestObjects.Employee> pred) {
        Config config = getConfig();
        config.setProperty(ClusterProperty.INDEX_COPY_BEHAVIOR.getName(), copyBehavior.name());
        config.setProperty(QueryEngineImpl.DISABLE_MIGRATION_FALLBACK.getName(), "true");
        HecatonInstance instance = createHecatonInstance(config);
        IMap<Integer, SampleTestObjects.Employee> map = instance.getMap("default");

        map.addIndex(ordered ? IndexType.SORTED : IndexType.HASH, "date");
        for (int i = 10; i >= 1; i--) {
            Employee employee = new Employee(i, "name-" + i, i, true, i * 100);
            if (i % 2 == 0) {
                employee.setDate(new Timestamp(i * 1000000));
            } else {
                employee.setDate(null);
            }
            map.put(i, employee);
        }

        List<Long> dates = new ArrayList<>();
        for (SampleTestObjects.Employee employee : map.values(pred)) {
            Timestamp date = employee.getDate();
            dates.add(date == null ? null : date.getTime());
        }

        return dates;
    }
}
