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

package uk.org.hecaton.jet.sql.impl.connector.map.index;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.jet.sql.impl.support.expressions.ExpressionBiValue;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.sql.HecatonSqlException;
import uk.org.hecaton.sql.SqlResult;
import uk.org.hecaton.sql.SqlRow;
import uk.org.hecaton.sql.impl.QueryException;
import uk.org.hecaton.sql.impl.SqlErrorCode;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Simple test to verify the behavior in case of the converter type mismatch.
 * <p>
 * We put entries of different types to different members and observe that index lookup cannot be used due to mismatch.
 */
@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlIndexConverterMismatchTest extends SqlIndexTestSupport {

    private static final String MAP_NAME = "map";

    private final TestHecatonInstanceFactory factory = new TestHecatonInstanceFactory(2);
    private HecatonInstance member1;
    private HecatonInstance member2;
    private IMap<Integer, ExpressionBiValue> map;

    @Parameterized.Parameter
    public boolean composite;

    @Parameterized.Parameters(name = "composite:{0}")
    public static Collection<Object[]> parameters() {
        List<Object[]> res = new ArrayList<>();

        res.add(new Object[]{false});
        res.add(new Object[]{true});

        return res;
    }

    @Before
    public void before() {
        IndexConfig indexConfig = new IndexConfig().setName("index").setType(IndexType.HASH).addAttribute("field1");

        if (composite) {
            indexConfig.addAttribute("field2");
        }

        Config config = getConfig();
        config.addMapConfig(new MapConfig().setName(MAP_NAME).setBackupCount(0).addIndexConfig(indexConfig));

        member1 = factory.newHecatonInstance(config);
        member2 = factory.newHecatonInstance(config);

        map = member1.getMap(MAP_NAME);
    }

    @SuppressWarnings({"StatementWithEmptyBody", "JavadocReference"})
    @Ignore("https://github.com/hazelcast/hazelcast/issues/19287")
    @Test
    public void testMismatch() {
        ExpressionBiValue value1 = new ExpressionBiValue.IntegerIntegerVal();
        value1.field1(10);
        value1.field2(10);

        ExpressionBiValue value2 = new ExpressionBiValue.StringIntegerVal();
        value2.field1("10");
        value2.field2(10);

        map.put(getLocalKey(member1, key -> key), value1);
        map.put(getLocalKey(member2, key -> key), value2);

        try {
            try (SqlResult result = member1.getSql().execute("SELECT key FROM " + MAP_NAME + " WHERE field1=1")) {
                for (SqlRow ignore : result) {
                    // No-op.
                }
            }

            fail("Must fail!");
        } catch (HecatonSqlException e) {
            assertEquals(SqlErrorCode.INDEX_INVALID, e.getCode());
            Throwable ex = findRootQueryException(e);
            assertEquals("Cannot use the index \"index\" of the IMap \"map\" because it has component \"field1\" of type VARCHAR, but INTEGER was expected", ex.getMessage());
        }
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    private Throwable findRootQueryException(Throwable t) {
        while (t != null) {
            // we need exactly root exception for this particular case
            if (t instanceof QueryException && t.getCause() == null) {
                return t;
            }
            t = t.getCause();
        }
        return null;
    }
}
