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

package uk.org.hecaton.internal.serialization.impl.compact.integration;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.util.FilteringClassLoader;
import uk.org.hecaton.map.EntryProcessor;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecord;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecordBuilder;
import uk.org.hecaton.query.Predicates;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import example.serialization.EmployeeDTO;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class CompactFormatIntegrationTest extends HecatonTestSupport {

    protected TestHecatonFactory factory = new TestHecatonFactory();
    protected HecatonInstance instance1;
    protected HecatonInstance instance2;

    @Parameterized.Parameter(0)
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameter(1)
    public boolean serverDoesNotHaveClasses;

    @Parameterized.Parameters(name = "inMemoryFormat:{0}, serverDoesNotHaveClasses:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, true},
                {InMemoryFormat.BINARY, false},
                {InMemoryFormat.OBJECT, true},
                {InMemoryFormat.OBJECT, false}
        });
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig();
        config.getMapConfig("test").setInMemoryFormat(inMemoryFormat);
        if (serverDoesNotHaveClasses) {
            List<String> excludes = singletonList("example.serialization");
            FilteringClassLoader classLoader = new FilteringClassLoader(excludes, null);
            config.setClassLoader(classLoader);
        }
        return config;
    }

    @Before
    public abstract void setup();

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @Test
    public void testBasic() {
        Object value = newValue(30, 102310312);
        IMap<Integer, Object> map = instance1.getMap("test");
        map.put(1, value);

        IMap<Integer, Object> map2 = instance2.getMap("test");
        assertEquals(value, map2.get(1));
    }

    @Test
    public void testBasicQuery() {
        IMap<Integer, Object> map = instance1.getMap("test");
        for (int i = 0; i < 100; i++) {
            map.put(i, newValue(i, 102310312));
        }

        IMap<Integer, Object> map2 = instance2.getMap("test");
        int size = map2.keySet(Predicates.sql("age > 19")).size();
        assertEquals(80, size);
    }

    @Test
    public void testJoinedMemberQuery() {
        IMap<Integer, Object> map = instance1.getMap("test");
        for (int i = 0; i < 100; i++) {
            map.put(i, newValue(i, 102310312));
        }

        HecatonInstance newInstance = factory.newHecatonInstance(getConfig());

        waitClusterForSafeState(newInstance);

        IMap<Integer, Object> map2 = newInstance.getMap("test");
        int size = map2.keySet(Predicates.sql("age > 19")).size();
        assertEquals(80, size);
    }

    @Test
    public void testEntryProcessor() {
        IMap<Integer, Object> map = instance1.getMap("test");
        for (int i = 0; i < 100; i++) {
            map.put(i, newValue(i, 102310312));
        }

        IMap map2 = instance2.getMap("test");
        if (serverDoesNotHaveClasses) {
            map2.executeOnEntries(new GenericIncreaseAgeEntryProcessor());
        } else {
            map2.executeOnEntries(new IncreaseAgeEntryProcessor());
        }

        for (int i = 0; i < 100; i++) {
            if (serverDoesNotHaveClasses) {
                GenericRecord record = (GenericRecord) map2.get(i);
                assertEquals(record.getInt32("age"), 1000 + i);
            } else {
                EmployeeDTO employeeDTO = (EmployeeDTO) map.get(i);
                assertEquals(employeeDTO.getAge(), 1000 + i);
            }
        }
    }

    private Object newValue(int age, long id) {
        if (serverDoesNotHaveClasses) {
            return GenericRecordBuilder.compact("employee")
                    .setInt32("age", age)
                    .setInt64("id", id)
                    .build();
        } else {
            return new EmployeeDTO(age, id);
        }
    }

    static class IncreaseAgeEntryProcessor implements EntryProcessor<Integer, EmployeeDTO, Object>, Serializable {
        @Override
        public Object process(Map.Entry<Integer, EmployeeDTO> entry) {
            EmployeeDTO value = entry.getValue();
            value.setAge(value.getAge() + 1000);
            entry.setValue(value);
            return null;
        }
    }

    static class GenericIncreaseAgeEntryProcessor implements EntryProcessor<Integer, GenericRecord, Object>, Serializable {
        @Override
        public Object process(Map.Entry<Integer, GenericRecord> entry) {
            GenericRecord value = entry.getValue();
            GenericRecord newValue = value.newBuilderWithClone()
                    .setInt32("age", value.getInt32("age") + 1000)
                    .build();
            entry.setValue(newValue);
            return null;
        }
    }
}
