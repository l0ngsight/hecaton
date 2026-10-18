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
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.config.MapStoreConfig;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.MapStoreAdapter;
import uk.org.hecaton.map.listener.EntryExpiredListener;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.query.SampleTestObjects.Employee;
import uk.org.hecaton.query.SampleTestObjects.PortableEmployee;
import uk.org.hecaton.query.SampleTestObjects.ValueType;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static uk.org.hecaton.internal.util.RootCauseMatcher.rootCause;
import static uk.org.hecaton.map.impl.eviction.MapClearExpiredRecordsTask.PROP_CLEANUP_ENABLED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryAdvancedTest extends HecatonTestSupport {

    protected void configureMap(String name, Config config) {
        // do nothing
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Test
    public void testQueryOperationAreNotSentToLiteMembers() {
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory(2);
        String mapName = randomMapName();
        Config fullConfig = getConfig();
        configureMap(mapName, fullConfig);
        HecatonInstance fullMember = nodeFactory.newHecatonInstance(fullConfig);
        Config liteConfig = getConfig().setLiteMember(true);
        configureMap(mapName, liteConfig);
        nodeFactory.newHecatonInstance(liteConfig);

        assertClusterSizeEventually(2, fullMember);

        IMap<Integer, Integer> map = fullMember.getMap(mapName);
        DeserializationCountingPredicate predicate = new DeserializationCountingPredicate();

        // initialize all partitions
        for (int i = 0; i < 5000; i++) {
            map.put(i, i);
        }

        map.values(predicate);
        assertEquals(0, predicate.serializationCount());
    }

    public static class DeserializationCountingPredicate implements Predicate, DataSerializable {
        private static final AtomicInteger counter = new AtomicInteger();

        @Override
        public boolean apply(Map.Entry mapEntry) {
            return false;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            counter.incrementAndGet();
        }

        int serializationCount() {
            return counter.get();
        }
    }

    @Test
    public void testQueryWithTTL() {
        Config config = getConfig();
        // disable background expiration task to keep test safe from jitter
        config.setProperty(PROP_CLEANUP_ENABLED, "false");

        String mapName = "default";
        config.getMapConfig(mapName)
              .setTimeToLiveSeconds(3);
        configureMap(mapName, config);

        HecatonInstance instance = createHecatonInstance(config);

        IMap<String, Employee> map = instance.getMap(mapName);
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");

        int passiveEmployees = 5;
        int activeEmployees = 5;
        int allEmployees = passiveEmployees + activeEmployees;

        final CountDownLatch latch = new CountDownLatch(allEmployees);
        map.addEntryListener((EntryExpiredListener) event -> latch.countDown(), false);

        for (int i = 0; i < activeEmployees; i++) {
            Employee employee = new Employee("activeEmployee" + i, 60, true, i);
            map.put("activeEmployee" + i, employee);
        }

        for (int i = 0; i < passiveEmployees; i++) {
            Employee employee = new Employee("passiveEmployee" + i, 60, false, i);
            map.put("passiveEmployee" + i, employee);
        }

        // check the query result before eviction
        final Collection values = map.values(Predicates.sql("active"));
        assertTrueEventually(() -> assertEquals(String.format("Expected %s results but got %s."
                        + " Number of evicted entries: %s.",
                activeEmployees, values.size(), allEmployees - latch.getCount()),
                activeEmployees, values.size()));

        // delete expire keys by explicitly calling `map#get`
        assertTrueEventually(() -> {
            for (int i = 0; i < activeEmployees; i++) {
                assertNull(map.get("activeEmployee" + i));
            }

            for (int i = 0; i < passiveEmployees; i++) {
                assertNull(map.get("passiveEmployee" + i));
            }
        });

        // wait until eviction is completed
        assertOpenEventually(latch);

        // check the query result after eviction
        assertEquals(0, map.values(Predicates.sql("active")).size());
    }

    @Test
    public void testTwoNodesWithPartialIndexes() {
        Config config = getConfig();
        configureMap("employees", config);
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory(2);
        HecatonInstance instance1 = nodeFactory.newHecatonInstance(config);
        HecatonInstance instance2 = nodeFactory.newHecatonInstance(config);

        IMap<String, Employee> map = instance1.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");

        for (int i = 0; i < 500; i++) {
            Employee employee = new Employee(i, "name" + i % 100, "city" + (i % 100), i % 60, ((i & 1) == 1), i);
            map.put(String.valueOf(i), employee);
        }
        assertClusterSize(2, instance1, instance2);

        map = instance2.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");

        Collection<Employee> entries = map.values(Predicates.sql("name='name3' and city='city3' and age > 2"));
        assertEquals(5, entries.size());
        for (Employee employee : entries) {
            assertEquals("name3", employee.getName());
            assertEquals("city3", employee.getCity());
        }

        entries = map.values(Predicates.sql("name LIKE '%name3' and city like '%city3' and age > 2"));
        assertEquals(5, entries.size());
        for (Employee employee : entries) {
            assertEquals("name3", employee.getName());
            assertEquals("city3", employee.getCity());
            assertTrue(employee.getAge() > 2);
        }

        entries = map.values(Predicates.sql("name LIKE '%name3%' and city like '%city30%'"));
        assertEquals(5, entries.size());
        for (Employee employee : entries) {
            assertTrue(employee.getName().startsWith("name3"));
            assertTrue(employee.getCity().startsWith("city3"));
        }
    }

    @Test
    public void testTwoNodesWithIndexes() {
        Config config = getConfig();
        configureMap("employees", config);
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory(2);
        HecatonInstance instance1 = nodeFactory.newHecatonInstance(config);
        HecatonInstance instance2 = nodeFactory.newHecatonInstance(config);

        IMap<String, Employee> map = instance1.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.HASH, "city");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");

        for (int i = 0; i < 5000; i++) {
            Employee employee = new Employee(i, "name" + i % 100, "city" + (i % 100), i % 60, ((i & 1) == 1), i);
            map.put(String.valueOf(i), employee);
        }
        assertClusterSize(2, instance1, instance2);

        map = instance2.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.HASH, "city");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");

        Collection<Employee> entries = map.values(Predicates.sql("name='name3' and city='city3' and age > 2"));
        assertEquals(50, entries.size());
        for (Employee employee : entries) {
            assertEquals("name3", employee.getName());
            assertEquals("city3", employee.getCity());
        }

        entries = map.values(Predicates.sql("name LIKE '%name3' and city like '%city3' and age > 2"));
        assertEquals(50, entries.size());
        for (Employee employee : entries) {
            assertEquals("name3", employee.getName());
            assertEquals("city3", employee.getCity());
            assertTrue(employee.getAge() > 2);
        }

        entries = map.values(Predicates.sql("name LIKE '%name3%' and city like '%city30%'"));
        assertEquals(50, entries.size());
        for (Employee employee : entries) {
            assertTrue(employee.getName().startsWith("name3"));
            assertTrue(employee.getCity().startsWith("city3"));
        }
    }

    @Test
    public void testOneMemberWithoutIndex() {
        Config config = getConfig();
        configureMap("employees", config);
        HecatonInstance instance = createHecatonInstance(config);
        IMap<String, Employee> map = instance.getMap("employees");
        QueryBasicTest.doFunctionalQueryTest(map);
    }

    @Test
    public void testOneMemberWithIndex() {
        Config config = getConfig();
        configureMap("employees", config);
        HecatonInstance instance = createHecatonInstance(config);
        IMap<String, Employee> map = instance.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");
        QueryBasicTest.doFunctionalQueryTest(map);
    }

    @Test
    public void testOneMemberSQLWithoutIndex() {
        Config config = getConfig();
        configureMap("employees", config);
        HecatonInstance instance = createHecatonInstance(config);
        IMap<String, Employee> map = instance.getMap("employees");
        QueryBasicTest.doFunctionalSQLQueryTest(map);
        Set<Map.Entry<String, Employee>> entries = map.entrySet(Predicates.sql("active and age>23"));
        assertEquals(27, entries.size());
    }

    @Test
    public void testOneMemberSQLWithIndex() {
        Config config = getConfig();
        configureMap("employees", config);
        HecatonInstance instance = createHecatonInstance(config);
        IMap<String, Employee> map = instance.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");
        QueryBasicTest.doFunctionalSQLQueryTest(map);
    }

    @Test
    public void testTwoMembers() {
        Config config = getConfig();
        configureMap("employees", config);
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory(2);
        HecatonInstance instance = nodeFactory.newHecatonInstance(config);
        nodeFactory.newHecatonInstance(config);

        IMap<String, Employee> map = instance.getMap("employees");
        QueryBasicTest.doFunctionalQueryTest(map);
    }

    @Test
    public void testTwoMembersWithIndexes() {
        Config config = getConfig();
        configureMap("employees", config);
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory(2);
        HecatonInstance instance = nodeFactory.newHecatonInstance(config);
        nodeFactory.newHecatonInstance(config);

        IMap<String, Employee> map = instance.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");
        QueryBasicTest.doFunctionalQueryTest(map);
    }

    @Test
    public void testTwoMembersWithIndexesAndShutdown() {
        Config config = getConfig();
        configureMap("employees", config);
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory(2);
        HecatonInstance instance1 = nodeFactory.newHecatonInstance(config);
        HecatonInstance instance2 = nodeFactory.newHecatonInstance(config);

        IMap<String, Employee> map = instance1.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");

        QueryBasicTest.doFunctionalQueryTest(map);
        assertEquals(101, map.size());
        instance2.getLifecycleService().shutdown();
        assertEquals(101, map.size());

        Set<Map.Entry<String, Employee>> entries = map.entrySet(Predicates.sql("active and age=23"));
        assertEquals(2, entries.size());
        for (Map.Entry<String, Employee> entry : entries) {
            Employee employee = entry.getValue();
            assertEquals(23, employee.getAge());
            assertTrue(employee.isActive());
        }
    }

    @Test
    public void testTwoMembersWithIndexesAndShutdown2() {
        Config config = getConfig();
        configureMap("employees", config);
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory(2);
        HecatonInstance instance1 = nodeFactory.newHecatonInstance(config);
        HecatonInstance instance2 = nodeFactory.newHecatonInstance(config);

        IMap<String, Employee> map = instance1.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");

        QueryBasicTest.doFunctionalQueryTest(map);
        assertEquals(101, map.size());
        instance1.getLifecycleService().shutdown();

        map = instance2.getMap("employees");
        assertEquals(101, map.size());
        Set<Map.Entry<String, Employee>> entries = map.entrySet(Predicates.sql("active and age=23"));
        assertEquals(2, entries.size());
        for (Map.Entry<String, Employee> entry : entries) {
            Employee employee = entry.getValue();
            assertEquals(23, employee.getAge());
            assertTrue(employee.isActive());
        }
    }

    @Test
    public void testTwoMembersWithIndexesAndShutdown3() {
        Config config = getConfig();
        configureMap("employees", config);
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory(2);
        HecatonInstance instance1 = nodeFactory.newHecatonInstance(config);

        IMap<String, Employee> map = instance1.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");
        QueryBasicTest.doFunctionalQueryTest(map);

        assertEquals(101, map.size());
        HecatonInstance instance2 = nodeFactory.newHecatonInstance(config);
        assertEquals(101, map.size());

        instance1.getLifecycleService().shutdown();
        map = instance2.getMap("employees");
        assertEquals(101, map.size());

        Set<Map.Entry<String, Employee>> entries = map.entrySet(Predicates.sql("active and age=23"));
        assertEquals(2, entries.size());
        for (Map.Entry<String, Employee> entry : entries) {
            Employee employee = entry.getValue();
            assertEquals(23, employee.getAge());
            assertTrue(employee.isActive());
        }
    }

    @Test
    public void testSecondMemberAfterAddingIndexes() {
        Config config = getConfig();
        configureMap("employees", config);
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory(2);
        HecatonInstance instance = nodeFactory.newHecatonInstance(config);
        IMap<String, Employee> map = instance.getMap("employees");
        map.addIndex(IndexType.HASH, "name");
        map.addIndex(IndexType.SORTED, "age");
        map.addIndex(IndexType.HASH, "active");

        nodeFactory.newHecatonInstance(config);
        QueryBasicTest.doFunctionalQueryTest(map);
    }

    @Test
    public void testMapWithIndexAfterShutDown() {
        Config config = getConfig();
        String mapName = "default";
        config.getMapConfig(mapName).addIndexConfig(new IndexConfig(IndexType.HASH, "typeName"));
        configureMap(mapName, config);

        HecatonInstance[] instances = createHecatonInstanceFactory(3).newInstances(config);

        final IMap<Integer, ValueType> map = instances[0].getMap(mapName);
        final int sampleSize1 = 100;
        final int sampleSize2 = 30;
        int totalSize = sampleSize1 + sampleSize2;

        for (int i = 0; i < sampleSize1; i++) {
            map.put(i, new ValueType("type" + i));
        }

        for (int i = sampleSize1; i < totalSize; i++) {
            map.put(i, new ValueType("typex"));
        }

        Collection typexValues = map.values(Predicates.sql("typeName = typex"));
        assertEquals(sampleSize2, typexValues.size());

        instances[1].shutdown();

        assertEquals(totalSize, map.size());
        assertTrueEventually(() -> {
            final Collection values = map.values(Predicates.sql("typeName = typex"));
            assertEquals(sampleSize2, values.size());
        });

        instances[2].shutdown();

        assertEquals(totalSize, map.size());
        assertTrueEventually(() -> {
            final Collection values = map.values(Predicates.sql("typeName = typex"));
            assertEquals(sampleSize2, values.size());
        });
    }

    // issue 1404 "to be fixed by issue 1404"
    @Test
    public void testQueryAfterInitialLoad() {
        final int size = 100;
        String name = "default";
        Config config = getConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new MapStoreAdapter<Integer, Employee>() {
            @Override
            public Map<Integer, Employee> loadAll(Collection<Integer> keys) {
                Map<Integer, Employee> map = new HashMap<>();
                for (Integer key : keys) {
                    Employee emp = new Employee();
                    emp.setActive(true);
                    map.put(key, emp);
                }
                return map;
            }

            @Override
            public Set<Integer> loadAllKeys() {
                Set<Integer> set = new HashSet<>();
                for (int i = 0; i < size; i++) {
                    set.add(i);
                }
                return set;
            }
        });

        config.getMapConfig(name).setMapStoreConfig(mapStoreConfig);
        configureMap(name, config);
        HecatonInstance instance = createHecatonInstance(config);
        final IMap map = instance.getMap(name);
        assertTrueEventually(() -> {
            Collection values = map.values(Predicates.sql("active = true"));
            assertEquals(size, values.size());
        });
    }

    @Test
    // see: https://github.com/hazelcast/hazelcast/issues/3927
    public void testUnknownPortableField_notCausesQueryException_withoutIndex() {
        String mapName = randomMapName();
        Config config = getConfig();
        config.getSerializationConfig().addPortableFactory(666, classId -> new PortableEmployee());
        configureMap(mapName, config);
        HecatonInstance hecatonInstance = createHecatonInstance(config);

        IMap<Integer, PortableEmployee> map = hecatonInstance.getMap(mapName);
        for (int i = 0; i < 5; i++) {
            map.put(i, new PortableEmployee(i, "name_" + i));
        }

        Collection values = map.values(Predicates.sql("notExist = name_0 OR a > 1"));
        assertEquals(3, values.size());
    }

    @Test
    // see: https://github.com/hazelcast/hazelcast/issues/3927
    public void testUnknownPortableField_notCausesQueryException_withIndex() {
        String mapName = "default";
        Config config = getConfig();
        config.getSerializationConfig().addPortableFactory(666, classId -> new PortableEmployee());
        config.getMapConfig(mapName)
              .addIndexConfig(new IndexConfig(IndexType.HASH, "notExist"))
              .addIndexConfig(new IndexConfig(IndexType.HASH, "n"));
        configureMap(mapName, config);

        HecatonInstance hecatonInstance = createHecatonInstance(config);

        IMap<Integer, PortableEmployee> map = hecatonInstance.getMap(mapName);
        for (int i = 0; i < 5; i++) {
            map.put(i, new PortableEmployee(i, "name_" + i));
        }

        Collection values = map.values(Predicates.sql("n = name_2 OR notExist = name_0"));
        assertEquals(1, values.size());
    }

    @Test
    public void testClassNotFoundErrorDelegatedToCallerOnQuery() {
        Config config = getConfig();
        configureMap("map", config);
        HecatonInstance hecatonInstance = createHecatonInstance(config);

        IMap<Integer, Integer> map = hecatonInstance.getMap("map");

        map.put(1, 1);
        //A remote predicate can throw Error in case of Usercodeployment and missing sub classes
        //See the issue for actual problem https://github.com/hazelcast/hazelcast/issues/18052
        //We are throwing error to see if the error is delegated to the caller
        assertThatThrownBy(() -> map.values(new ErrorThrowingPredicate()))
                .isInstanceOf(HecatonException.class)
                .cause().has(rootCause(NoClassDefFoundError.class));
    }
}
