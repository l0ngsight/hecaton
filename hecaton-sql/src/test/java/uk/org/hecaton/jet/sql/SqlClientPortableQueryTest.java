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

import com.google.common.collect.Iterators;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.nio.serialization.ClassDefinition;
import uk.org.hecaton.nio.serialization.ClassDefinitionBuilder;
import uk.org.hecaton.nio.serialization.Portable;
import uk.org.hecaton.nio.serialization.PortableFactory;
import uk.org.hecaton.nio.serialization.PortableReader;
import uk.org.hecaton.nio.serialization.PortableWriter;
import uk.org.hecaton.sql.SqlResult;
import uk.org.hecaton.sql.SqlRow;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.NightlyTest;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

/**
 * Verifies that the member can extract fields from PortableGenericRecords
 * when it does not have the necessary PortableFactory in its config.
 * The test is moved to NightlyTest suite because Portable is deprecated,
 * will be removed in the future, and the test itself lasts for ~40s on lab.
 */
@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({NightlyTest.class, ParallelJVMTest.class})
public class SqlClientPortableQueryTest extends SqlTestSupport {

    private static final int PORTABLE_FACTORY_ID = 1;
    private static final int PORTABLE_CHILD_CLASS_ID = 1;
    private static final int PORTABLE_PARENT_CLASS_ID = 2;

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameter(1)
    public boolean clusterHasPortableConfig;

    public TestHecatonFactory factory = new TestHecatonFactory();

    @Parameterized.Parameters(name = "inMemoryFormat:{0}, clusterHasPortableConfig:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY, true},
                {InMemoryFormat.BINARY, false},
                {InMemoryFormat.OBJECT, true},
                {InMemoryFormat.OBJECT, false}
        });
    }

    @Before
    public void setup() {
        MapConfig mapConfig = new MapConfig("default").setInMemoryFormat(inMemoryFormat);
        Config config = smallInstanceConfig().addMapConfig(mapConfig);
        if (clusterHasPortableConfig) {
            config.getSerializationConfig().addPortableFactory(PORTABLE_FACTORY_ID, new TestPortableFactory());
        }

        ClassDefinition childClassDefinition = new ClassDefinitionBuilder(PORTABLE_FACTORY_ID, PORTABLE_CHILD_CLASS_ID, 0)
                .addIntField("i")
                .addIntArrayField("ia")
                .build();
        ClassDefinition parentClassDefinition = new ClassDefinitionBuilder(PORTABLE_FACTORY_ID, PORTABLE_PARENT_CLASS_ID, 0)
                .addPortableField("child", childClassDefinition)
                .addIntField("id")
                .build();
        config.getSerializationConfig().addClassDefinition(childClassDefinition);
        config.getSerializationConfig().addClassDefinition(parentClassDefinition);

        factory.newHecatonInstance(config);
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testQueryOnPrimitive() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().addPortableFactory(PORTABLE_FACTORY_ID, new TestPortableFactory());
        HecatonInstance client = factory.newHecatonClient(clientConfig);
        createMapping(client, "test", int.class, PORTABLE_FACTORY_ID, PORTABLE_CHILD_CLASS_ID, 0);
        IMap<Integer, Object> map = client.getMap("test");
        fillMap(map, 50, ChildPortable::new);

        SqlResult result = client.getSql().execute("SELECT * FROM test WHERE i >= 45");

        assertThat(result).hasSize(5);
    }

    @Test
    public void testQueryOnObject() {
        //To be able to run comparison methods on objects on the server we need the classes
        assumeTrue(clusterHasPortableConfig);
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().addPortableFactory(PORTABLE_FACTORY_ID, new TestPortableFactory());
        HecatonInstance client = factory.newHecatonClient(clientConfig);
        createMapping(client, "test", int.class, PORTABLE_FACTORY_ID, PORTABLE_PARENT_CLASS_ID, 0);
        IMap<Integer, ParentPortable> map = client.getMap("test");
        fillMap(map, 100, ParentPortable::new);

        ChildPortable expected = new ChildPortable(10);
        SqlResult rows = client.getSql().execute("SELECT id FROM test WHERE child = ?", expected);

        SqlRow row = Iterators.getOnlyElement(rows.iterator());
        assertEquals(Integer.valueOf(10), row.getObject("id"));
    }

    @Test
    public void testNestedPortableAsColumn() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getSerializationConfig().addPortableFactory(PORTABLE_FACTORY_ID, new TestPortableFactory());
        HecatonInstance client = factory.newHecatonClient(clientConfig);
        createMapping(client, "test", int.class, PORTABLE_FACTORY_ID, PORTABLE_PARENT_CLASS_ID, 0);
        IMap<Integer, ParentPortable> map = client.getMap("test");
        fillMap(map, 100, ParentPortable::new);

        SqlResult result = client.getSql().execute("SELECT id, child FROM test WHERE id = ? ", 1);

        SqlRow row = Iterators.getOnlyElement(result.iterator());
        assertEquals((Integer) 1, row.getObject(0));
        assertEquals(new ChildPortable(1), row.getObject(1));
    }

    private static <T> void fillMap(IMap<Integer, T> map, int count, Function<Integer, T> constructor) {
        for (int i = 0; i < count; i++) {
            map.put(i, constructor.apply(i));
        }
    }

    private static class ChildPortable implements Portable, Comparable<ChildPortable> {
        private int i;
        private int[] ia;

        ChildPortable() {
        }

        ChildPortable(int i) {
            this.i = i;
            this.ia = new int[]{i};
        }

        @Override
        public int getFactoryId() {
            return PORTABLE_FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return PORTABLE_CHILD_CLASS_ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeInt("i", i);
            writer.writeIntArray("ia", ia);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            i = reader.readInt("i");
            ia = reader.readIntArray("ia");
        }

        @Override
        public int compareTo(ChildPortable o) {
            return i - o.i;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ChildPortable that = (ChildPortable) o;
            return i == that.i && Arrays.equals(ia, that.ia);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(i);
            result = 31 * result + Arrays.hashCode(ia);
            return result;
        }
    }

    private static class ParentPortable implements Portable, Comparable<ParentPortable> {
        private ChildPortable c;
        private int id;

        ParentPortable() {
        }

        ParentPortable(int i) {
            this.c = new ChildPortable(i);
            this.id = i;
        }

        @Override
        public int getFactoryId() {
            return PORTABLE_FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return PORTABLE_PARENT_CLASS_ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writePortable("child", c);
            writer.writeInt("id", id);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            c = reader.readPortable("child");
            id = reader.readInt("id");
        }

        @Override
        public int compareTo(ParentPortable o) {
            return c.compareTo(o.c);
        }
    }

    private static class TestPortableFactory implements PortableFactory {
        @Override
        public Portable create(int classId) {
            if (classId == PORTABLE_CHILD_CLASS_ID) {
                return new ChildPortable();
            } else if (classId == PORTABLE_PARENT_CLASS_ID) {
                return new ParentPortable();
            }
            return null;
        }
    }
}
