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

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.connection.ClientConnection;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.SqlExecute_reservedCodec;
import uk.org.hecaton.client.util.ConfigRoutingUtil;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.nio.serialization.HecatonSerializationException;
import uk.org.hecaton.sql.HecatonSqlException;
import uk.org.hecaton.sql.SqlResult;
import uk.org.hecaton.sql.SqlRow;
import uk.org.hecaton.sql.SqlStatement;
import uk.org.hecaton.sql.impl.SqlErrorCode;
import uk.org.hecaton.sql.impl.client.SqlClientService;
import uk.org.hecaton.sql.impl.state.QueryClientStateRegistry;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

import static uk.org.hecaton.client.config.RoutingMode.ALL_MEMBERS;
import static uk.org.hecaton.client.config.RoutingMode.SINGLE_MEMBER;
import static uk.org.hecaton.sql.SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

/**
 * Test for different error conditions (client).
 */
@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlErrorClientTest extends SqlErrorAbstractTest {

    @Parameterized.Parameter
    public RoutingMode routingMode;

    @Parameterized.Parameters(name = "{index}: routingMode={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(SINGLE_MEMBER, ALL_MEMBERS);
    }

    @Override
    public void after() {
        client = null;
        super.after();
    }

    protected HecatonInstance client;

    protected Supplier<HecatonInstance> newClientSupplier() {
        return () -> factory.newHecatonClient(newClientConfig());
    }

    protected ClientConfig newClientConfig() {
        ClientConfig clientConfig = ConfigRoutingUtil.newClientConfig(routingMode);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig()
                .setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        return clientConfig;
    }

    @Test
    public void testTimeout_execute() {
        checkTimeout(newClientSupplier());
    }

    @Test
    public void testDataTypeMismatch() {
        checkDataTypeMismatch(newClientSupplier());
    }

    @Test
    public void testClientConnectedToLiteMember() {
        factory.newHecatonInstance(getConfig().setLiteMember(true));
        client = factory.newHecatonClient(null);

        HecatonSqlException error = assertSqlException(client, query());
        assertErrorCode(SqlErrorCode.GENERIC, error);
        assertEquals("SQL queries cannot be executed on lite members", error.getMessage());
    }

    @Test
    public void testParsingError() {
        checkParsingError(newClientSupplier());
    }

    @Test
    public void testUserCancel() {
        checkUserCancel(newClientSupplier());
    }

    /**
     * Test proper handling of member disconnect while waiting for execute result.
     */
    @Test
    public void testMemberDisconnect_execute() {
        instance1 = newHecatonInstance(true);
        client = newClientSupplier().get();

        SqlStatement streamingQuery = new SqlStatement("SELECT * FROM TABLE(GENERATE_STREAM(5000))");

        HecatonSqlException error = assertSqlExceptionWithShutdown(client, streamingQuery);
        assertErrorCode(SqlErrorCode.CONNECTION_PROBLEM, error);
    }

    @Test
    public void testMemberDisconnect_fetch() {
        instance1 = newHecatonInstance(true);
        client = newClientSupplier().get();

        createMapping(instance1, MAP_NAME, long.class, long.class);
        populate(instance1, DEFAULT_CURSOR_BUFFER_SIZE + 1);

        // Get the first row.
        boolean shutdown = true;

        try {
            for (SqlRow ignore : client.getSql().execute(query())) {
                // Shutdown the member
                if (shutdown) {
                    instance1.shutdown();

                    shutdown = false;
                }
            }

            fail("Should fail");
        } catch (HecatonSqlException e) {
            assertErrorCode(SqlErrorCode.CONNECTION_PROBLEM, e);
        }
    }

    @Test
    public void testMemberDisconnect_close() {
        instance1 = newHecatonInstance(true);
        client = newClientSupplier().get();

        createMapping(instance1, MAP_NAME, long.class, long.class);
        populate(instance1, DEFAULT_CURSOR_BUFFER_SIZE + 1);

        try {
            SqlResult result = client.getSql().execute(query());

            instance1.shutdown();

            for (SqlRow ignore : result) {
                // No-op.
            }

            result.close();

            fail("Should fail");
        } catch (HecatonSqlException e) {
            assertErrorCode(SqlErrorCode.CONNECTION_PROBLEM, e);
        }
    }

    /**
     * Make sure that client cursors are cleared up eventually on client stop.
     */
    @Test
    public void testCursorCleanupOnClientLeave() {
        instance1 = newHecatonInstance(true);
        client = newClientSupplier().get();

        Map<Integer, Integer> localMap = new HashMap<>();
        Map<Integer, Integer> map = instance1.getMap(MAP_NAME);

        for (int i = 0; i < DEFAULT_CURSOR_BUFFER_SIZE + 1; i++) {
            localMap.put(i, i);
        }

        createMapping(instance1, MAP_NAME, int.class, int.class);
        map.putAll(localMap);

        QueryClientStateRegistry cursorRegistry = sqlInternalService(instance1).getClientStateRegistry();

        // Create dangling cursor
        client.getSql().execute("SELECT * FROM " + MAP_NAME);

        assertTrueEventually(() -> assertEquals(1, cursorRegistry.getCursorCount()));

        // Ensure that the cursor is cleared on client shutdown
        client.shutdown();
        assertTrueEventually(() -> assertEquals(0, cursorRegistry.getCursorCount()));
    }

    @Test
    public void testParameterError_serialization() {
        instance1 = newHecatonInstance(true);
        client = newClientSupplier().get();

        SqlStatement query = new SqlStatement("SELECT * FROM map").addParameter(new BadParameter(true, false));

        HecatonSqlException error = assertSqlException(client, query);
        assertErrorCode(SqlErrorCode.GENERIC, error);
        assertTrue(error.getMessage().contains("Failed to serialize query parameter"));
    }

    @Test
    public void testParameterError_deserialization() {
        instance1 = newHecatonInstance(true);
        client = newClientSupplier().get();

        SqlStatement query = new SqlStatement("SELECT * FROM map").addParameter(new BadParameter(false, true));

        HecatonSqlException error = assertSqlException(client, query);
        assertErrorCode(SqlErrorCode.GENERIC, error);
        assertTrue(error.getMessage().contains("Read error"));
    }

    @Test
    public void testRowError_deserialization() {
        try {
            instance1 = newHecatonInstance(true);
            client = newClientSupplier().get();

            Map<Integer, BadValue> localMap = new HashMap<>();
            IMap<Integer, BadValue> map = instance1.getMap(MAP_NAME);

            createMapping(instance1, MAP_NAME, int.class, BadValue.class);
            for (int i = 0; i < DEFAULT_CURSOR_BUFFER_SIZE + 1; i++) {
                localMap.put(i, new BadValue());
            }

            map.putAll(localMap);

            try (SqlResult result = client.getSql().execute("SELECT __key, this FROM " + MAP_NAME)) {
                Iterator<SqlRow> iterator = result.iterator();

                SqlRow firstRow = iterator.next();
                firstRow.getObject("__key");
                firstRow.getObject("this");

                BadValue.READ_ERROR.set(true);
                SqlRow secondRow = iterator.next();
                secondRow.getObject("__key");
                assertThatThrownBy(() -> secondRow.getObject("this"))
                        .isInstanceOf(HecatonSerializationException.class)
                        .hasMessageContaining("Failed to deserialize query result value");
            }
        } finally {
            BadValue.READ_ERROR.set(false);
        }
    }

    @Test
    public void testMissingHandler() {
        instance1 = newHecatonInstance(true);
        client = newClientSupplier().get();

        try {
            ClientMessage message = SqlExecute_reservedCodec.encodeRequest(
                "SELECT * FROM table",
                Collections.emptyList(),
                100L,
                100
            );

            SqlClientService clientService = ((SqlClientService) client.getSql());

            ClientConnection connection = clientService.getQueryConnection();
            clientService.invokeOnConnection(connection, message);

            fail("Must fail");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Unrecognized client message received"));
        }
    }

    private static class BadValue implements DataSerializable {

        private static final ThreadLocal<Boolean> READ_ERROR = ThreadLocal.withInitial(() -> false);

        @Override
        public void writeData(ObjectDataOutput out) {
            // No-op.
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            if (READ_ERROR.get()) {
                throw new IOException("Read error");
            }
        }
    }

    @SuppressWarnings("unused")
    private static class BadParameter implements DataSerializable {
        private boolean writeError;
        private boolean readError;

        @SuppressWarnings("checkstyle:RedundantModifier")
        public BadParameter() {
            // No-op.
        }

        private BadParameter(boolean writeError, boolean readError) {
            this.writeError = writeError;
            this.readError = readError;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            if (writeError) {
                throw new IOException("Write error");
            }

            out.writeBoolean(readError);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            readError = in.readBoolean();

            if (readError) {
                throw new IOException("Read error");
            }
        }
    }
}
