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

package uk.org.hecaton.jet.sql.impl;

import uk.org.hecaton.jet.sql.SqlTestSupport;
import uk.org.hecaton.sql.impl.InternalSqlService;
import uk.org.hecaton.sql.impl.SqlServiceImpl;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonSerialClassRunner.class)
public class SqlServiceImplInstantiationTest extends SqlTestSupport {

    @BeforeClass
    public static void setUpClass() {
        initialize(1, null);
    }

    @Test
    public void testCreate() {
        InternalSqlService sqlService = Accessors.getNodeEngineImpl(instance()).getSqlService();

        assertNotNull(sqlService);
        assertEquals(SqlServiceImpl.class, sqlService.getClass());
    }
}
