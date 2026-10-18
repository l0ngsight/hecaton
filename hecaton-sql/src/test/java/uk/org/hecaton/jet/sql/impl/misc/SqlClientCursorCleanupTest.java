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

package uk.org.hecaton.jet.sql.impl.misc;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.sql.SqlTestSupport;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.sql.SqlResult;
import uk.org.hecaton.sql.SqlRow;
import uk.org.hecaton.sql.SqlStatement;
import uk.org.hecaton.sql.impl.SqlInternalService;
import uk.org.hecaton.sql.impl.SqlServiceImpl;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HecatonSerialClassRunner.class)
public class SqlClientCursorCleanupTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";

    private static volatile boolean fail;

    private final TestHecatonFactory factory = new TestHecatonFactory();

    private HecatonInstance member;
    private HecatonInstance client;

    @Before
    public void before() {
        member = factory.newHecatonInstance(smallInstanceConfig());
        client = factory.newHecatonClient(new ClientConfig());

        member.getConfig().addMapConfig(new MapConfig().setName(MAP_NAME).setInMemoryFormat(InMemoryFormat.OBJECT));
    }

    @After
    public void after() {
        fail = false;

        factory.shutdownAll();

        member = null;
        client = null;
    }

    @Test
    public void testExceptionOnExecute() {
        IMap<Integer, Person> map = member.getMap(MAP_NAME);
        map.put(0, new Person());
        map.put(1, new Person());

        fail = true;

        try {
            SqlResult result = client.getSql().execute(statement());

            for (SqlRow ignore : result) {
                // No-op.
            }

            fail("Must fail");
        } catch (Exception e) {
            assertNoState();
        }
    }

    @Test
    public void testExceptionOnFetch() {
        IMap<Integer, Person> map = member.getMap(MAP_NAME);
        map.put(0, new Person());
        map.put(1, new Person());
        map.put(2, new Person());

        try {
            SqlResult result = client.getSql().execute(statement());

            for (SqlRow ignore : result) {
                fail = true;
            }

            fail("Must fail");
        } catch (Exception e) {
            assertNoState();
        }
    }

    private void assertNoState() {
        SqlInternalService service = ((SqlServiceImpl) member.getSql()).getInternalService();

        assertEquals(0, service.getResultRegistry().getResultCount());
        assertEquals(0, service.getClientStateRegistry().getCursorCount());
    }

    private static SqlStatement statement() {
        return new SqlStatement("SELECT * FROM " + MAP_NAME).setCursorBufferSize(1);
    }

    public static class Person implements DataSerializable {

        public Person() {
            // No-op
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            if (fail) {
                throw new IOException();
            }
        }

        @Override
        public void readData(ObjectDataInput in) {
            // No-op
        }
    }
}
