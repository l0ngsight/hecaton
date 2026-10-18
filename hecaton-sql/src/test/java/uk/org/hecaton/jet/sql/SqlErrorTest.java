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

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.core.MemberLeftException;
import uk.org.hecaton.jet.core.TopologyChangedException;
import uk.org.hecaton.jet.impl.execution.ExecutionContext;
import uk.org.hecaton.jet.sql.impl.connector.test.TestStreamSqlConnector;
import uk.org.hecaton.sql.HecatonSqlException;
import uk.org.hecaton.sql.SqlService;
import uk.org.hecaton.sql.SqlStatement;
import uk.org.hecaton.sql.impl.SqlErrorCode;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static uk.org.hecaton.sql.impl.type.QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE;
import static uk.org.hecaton.sql.impl.type.QueryDataTypeFamily.VARCHAR;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for different error conditions.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlErrorTest extends SqlErrorAbstractTest {

    @Test
    public void testTimeout() {
        checkTimeout(null);
    }

    @Test
    public void testMemberLeave() throws InterruptedException {
        // Start two instances
        instance1 = newHecatonInstance(false);
        instance2 = newHecatonInstance(true);

        String name = createTable(instance1.getSql(),
                row(timestampTz(0), "Alice", 1),
                row(timestampTz(1), null, 1),
                row(timestampTz(2), "Alice", 1),
                row(timestampTz(3), "Bob", 1),
                row(timestampTz(4), "Alice", 1),
                row(timestampTz(20), null, null)
        );

        Thread shutdownThread = new Thread(() -> {
            // Waiting for a deployment of the job on both members.
            assertTrueEventually(() -> {
                assertEquals(1, getExecutionContextCount(instance1));
                assertEquals(1, getExecutionContextCount(instance2));
            });
            instance2.shutdown();
        });
        shutdownThread.start();

        // Start query
        SqlStatement streamingQuery = new SqlStatement("SELECT window_start/*, window_end*/ FROM " +
                "TABLE(HOP(" +
                "  (SELECT * FROM TABLE(IMPOSE_ORDER(TABLE " + name + ", DESCRIPTOR(ts), INTERVAL '0.002' SECOND)))" +
                "  , DESCRIPTOR(ts)" +
                "  , INTERVAL '0.004' SECOND" +
                "  , INTERVAL '0.002' SECOND" +
                ")) " +
                "GROUP BY 1/*, 2*/");
        HecatonSqlException error = assertSqlException(instance1, streamingQuery);
        shutdownThread.join();
        assertInstanceOf(MemberLeftException.class, findRootCause(error));
        shutdownThread.join();
    }

    private static String createTable(SqlService sqlService, Object[]... values) {
        String name = randomName();
        TestStreamSqlConnector.create(
                sqlService,
                name,
                asList("ts", "name", "distance"),
                asList(TIMESTAMP_WITH_TIME_ZONE, VARCHAR, INTEGER),
                values
        );
        return name;
    }

    @Test
    public void testMemberLeaveDuringQueryAfterImmediateShutdown() {
        // Start two instances
        instance1 = newHecatonInstance(false);
        instance2 = newHecatonInstance(true);

        SqlStatement streamingQuery = new SqlStatement("SELECT * FROM TABLE(GENERATE_STREAM(1000))");

        // Start query with immediate shutdown afterwards
        HecatonSqlException error = assertSqlExceptionWithShutdown(instance1, streamingQuery);
        var rootCause = findRootCause(error);

        assertTrue(rootCause instanceof TopologyChangedException || rootCause instanceof HecatonInstanceNotActiveException);
    }

    @Test
    public void testDataTypeMismatch() {
        checkDataTypeMismatch(null);
    }

    @Test
    public void testExecuteOnLiteMember() {
        // Start one normal member and one local member.
        newHecatonInstance(true);
        HecatonInstance liteMember = factory.newHecatonInstance(getConfig().setLiteMember(true));

        // Insert data
        populate(liteMember);

        // Try query from the lite member.
        HecatonSqlException error = assertSqlException(liteMember, query());
        assertErrorCode(SqlErrorCode.GENERIC, error);
        assertEquals("SQL queries cannot be executed on lite members", error.getMessage());
    }

    @Test
    public void testParsingError() {
        checkParsingError(null);
    }

    @Test
    public void testUserCancel() {
        checkUserCancel(null);
    }

    /**
     * Returns the count of {@link ExecutionContext} that are running on given {@code instance}.
     */
    private static int getExecutionContextCount(HecatonInstance instance) {
        return getJetServiceBackend(instance).getJobExecutionService().getExecutionContexts().size();
    }
}
