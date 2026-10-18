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
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.PredicateBuilder.EntryObject;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.query.SampleTestObjects.Employee;
import uk.org.hecaton.query.SampleTestObjects.SpiedEmployee;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryIndexingTest extends HecatonTestSupport {

    private int count = 2000;
    private Map<Integer, Employee> employees;

    private TestHecatonInstanceFactory nodeFactory;
    private HecatonInstance h1;
    private HecatonInstance h2;

    private Predicate predicate;

    @Before
    public void setUp() {
        employees = newEmployees(count);

        nodeFactory = createHecatonInstanceFactory(2);
        Config config = newConfig();
        h1 = nodeFactory.newHecatonInstance(config);
        h2 = nodeFactory.newHecatonInstance(config);

        EntryObject entryObject = Predicates.newPredicateBuilder().getEntryObject();
        predicate = entryObject.get("name").equal(null).and(entryObject.get("city").isNull());

        assertClusterSizeEventually(2, h1);
    }

    @After
    public void tearDown() {
        nodeFactory.shutdownAll();
    }

    @Test
    public void testResultsHaveNullFields_whenPredicateTestsForNull() {
        IMap<Integer, Employee> map = h1.getMap("employees");
        map.putAll(employees);
        waitAllForSafeState(h1, h2);

        Collection<Employee> matchingEntries = runQueryNTimes(3, h2.getMap("employees"));

        assertEquals(count / 2, matchingEntries.size());
        // N queries result in getters called N times
        assertGettersCalledNTimes(matchingEntries, 3);
        assertFieldsAreNull(matchingEntries);
    }

    @Test
    public void testResultsHaveNullFields_whenUsingIndexes() {
        IMap<Integer, Employee> map = h1.getMap("employees");

        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "city");

        map.putAll(employees);
        waitAllForSafeState(h1, h2);

        Collection<Employee> matchingEntries = runQueryNTimes(3, h2.getMap("employees"));
        assertEquals(count / 2, matchingEntries.size());

        assertFieldsAreNull(matchingEntries);
    }

    private static Map<Integer, Employee> newEmployees(int employeeCount) {
        Map<Integer, Employee> employees = new HashMap<>();
        for (int i = 0; i < employeeCount; i++) {
            Employee spy;
            if (i % 2 == 0) {
                spy = new SpiedEmployee(i, null, null, 0, true, i);
            } else {
                spy = new SpiedEmployee(i, "name" + i, "city" + i, 0, true, i);
            }
            employees.put(i, spy);
        }
        return employees;
    }

    private static Config newConfig() {
        Config conf = new Config();
        conf.setProperty(QueryEngineImpl.DISABLE_MIGRATION_FALLBACK.getName(), "true");
        conf.getMapConfig("employees").setInMemoryFormat(InMemoryFormat.OBJECT).setBackupCount(0);
        // disabling replication since we don't use backups in this test
        conf.setProperty(ClusterProperty.PARTITION_MAX_PARALLEL_REPLICATIONS.getName(), "0");
        return conf;
    }

    private Collection<Employee> runQueryNTimes(int queryCount, IMap<String, Employee> map) {
        Collection<Employee> result = emptyList();
        for (int i = 0; i < queryCount; i++) {
            result = map.values(predicate);
        }
        return result;
    }

    private static void assertGettersCalledNTimes(Collection<Employee> matchingEmployees, int expectedCalls) {
        for (Employee employee : matchingEmployees) {
            SpiedEmployee spy = (SpiedEmployee) employee;
            assertEquals(expectedCalls, spy.getInvocationCount("getCity"));
            assertEquals(expectedCalls, spy.getInvocationCount("getName"));
        }
    }

    private static void assertFieldsAreNull(Collection<Employee> matchingEmployees) {
        for (Employee employee : matchingEmployees) {
            assertNull("city", employee.getCity());
            assertNull("name", employee.getName());
        }
    }
}
