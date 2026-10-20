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

import uk.org.hecaton.client.impl.connection.ClientConnection;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.SqlCloseCodec;
import uk.org.hecaton.client.impl.protocol.codec.SqlExecuteCodec;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.sql.SqlExpectedResultType;
import uk.org.hecaton.sql.impl.QueryId;
import uk.org.hecaton.sql.impl.SqlErrorCode;
import uk.org.hecaton.sql.impl.SqlServiceImpl;
import uk.org.hecaton.sql.impl.client.SqlClientService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Tests for a race condition between "execute" and "close" requests.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlClientExecuteCloseRaceTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";
    private static final String SQL = "SELECT * FROM " + MAP_NAME;

    private final TestHecatonFactory factory = new TestHecatonFactory();

    private SqlServiceImpl memberService;
    private SqlClientService clientService;

    @Before
    public void before() {
        HecatonInstance member = factory.newHecatonInstance();
        HecatonInstance client = factory.newHecatonClient();

        createMapping(member, MAP_NAME, int.class, int.class);
        Map<Integer, Integer> map = member.getMap(MAP_NAME);
        map.put(0, 0);
        map.put(1, 1);

        memberService = (SqlServiceImpl) member.getSql();
        clientService = (SqlClientService) client.getSql();
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void testExecuteClose() {
        QueryId queryId = QueryId.create(UUID.randomUUID());

        // Send "execute"
        ClientConnection connection = clientService.getQueryConnection();

        ClientMessage executeResponse = sendExecuteRequest(connection, queryId);

        checkExecuteResponse(executeResponse, true);

        assertEquals(1, memberService.getInternalService().getClientStateRegistry().getCursorCount());

        // Send "close"
        ClientMessage closeRequest = SqlCloseCodec.encodeRequest(queryId);

        clientService.invokeOnConnection(connection, closeRequest);

        assertEquals(0, memberService.getInternalService().getClientStateRegistry().getCursorCount());
    }

    @Test
    public void testCloseExecute() {
        QueryId queryId = QueryId.create(UUID.randomUUID());

        // Send "close"
        ClientConnection connection = clientService.getQueryConnection();

        ClientMessage closeRequest = SqlCloseCodec.encodeRequest(queryId);

        clientService.invokeOnConnection(connection, closeRequest);

        assertEquals(1, memberService.getInternalService().getClientStateRegistry().getCursorCount());

        // Send "execute"
        ClientMessage executeResponse = sendExecuteRequest(connection, queryId);

        assertEquals(0, memberService.getInternalService().getClientStateRegistry().getCursorCount());

        checkExecuteResponse(executeResponse, false);
    }

    @Test
    public void testClose() {
        QueryId queryId = QueryId.create(UUID.randomUUID());

        // Send "close"
        ClientConnection connection = clientService.getQueryConnection();

        ClientMessage closeRequest = SqlCloseCodec.encodeRequest(queryId);

        clientService.invokeOnConnection(connection, closeRequest);

        // Make sure that we observed the cancel request.
        assertEquals(1, memberService.getInternalService().getClientStateRegistry().getCursorCount());

        // Wait for it to disappear.
        memberService.getInternalService().getClientStateRegistry().setClosedCursorCleanupTimeoutSeconds(1L);

        assertTrueEventually(() -> assertEquals(0, memberService.getInternalService().getClientStateRegistry().getCursorCount()));
    }

    private void checkExecuteResponse(ClientMessage executeResponse, boolean success) {
        SqlExecuteCodec.ResponseParameters executeResponse0 = SqlExecuteCodec.decodeResponse(executeResponse);

        if (success) {
            assertNull(executeResponse0.error);
        } else {
            assertNotNull(executeResponse0.error);

            assertEquals(SqlErrorCode.CANCELLED_BY_USER, executeResponse0.error.getCode());
        }
    }

    private ClientMessage sendExecuteRequest(ClientConnection connection, QueryId queryId) {
        ClientMessage executeRequest = SqlExecuteCodec.encodeRequest(
                SQL,
                Collections.emptyList(),
                0L,
                1,
                null,
                SqlExpectedResultType.ANY.getId(),
                queryId,
                false
        );

        return clientService.invokeOnConnection(connection, executeRequest);
    }
}
