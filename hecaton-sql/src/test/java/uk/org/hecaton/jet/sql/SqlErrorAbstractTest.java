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

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.partition.Partition;
import uk.org.hecaton.partition.PartitionService;
import uk.org.hecaton.sql.HecatonSqlException;
import uk.org.hecaton.sql.SqlResult;
import uk.org.hecaton.sql.SqlRow;
import uk.org.hecaton.sql.SqlStatement;
import uk.org.hecaton.sql.impl.SqlErrorCode;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import org.junit.After;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HecatonSerialClassRunner.class)
public abstract class SqlErrorAbstractTest extends SqlTestSupport {

    protected static final String MAP_NAME = "map";
    private static final int DATA_SET_SIZE = 100;

    protected final TestHecatonFactory factory = new TestHecatonFactory();

    protected HecatonInstance instance1;
    protected HecatonInstance instance2;

    @After
    public void after() {
        instance1 = null;
        instance2 = null;

        factory.shutdownAll();
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    protected void checkTimeout(Supplier<HecatonInstance> clientSupplier) {
        // Start two instances and fill them with data
        instance1 = newHecatonInstance(false);
        instance2 = newHecatonInstance(true);

        // Execute query on the instance1.
        SqlStatement query = new SqlStatement("select v from table(generate_stream(1))")
                .setTimeoutMillis(1L);
        HecatonSqlException error = assertSqlException(clientSupplier != null
                ? clientSupplier.get() : instance1, query);
        assertTrue(error.getMessage().contains("CANCEL_FORCEFUL"));
    }

    protected void checkDataTypeMismatch(Supplier<HecatonInstance> clientSupplier) {
        // Start two instances and fill them with data
        instance1 = newHecatonInstance(false);
        instance2 = newHecatonInstance(true);

        IMap<Long, Object> map = instance1.getMap(MAP_NAME);

        createMapping(instance1, MAP_NAME, long.class, long.class);
        for (long i = 0; i < DATA_SET_SIZE; i++) {
            Object value = i == 0 ? Long.toString(i) : i;

            map.put(i, value);
        }

        // Execute query.
        HecatonSqlException error = assertSqlException(clientSupplier != null
                ? clientSupplier.get() : instance1, query());

        assertErrorCode(SqlErrorCode.DATA_EXCEPTION, error);
        assertTrue(error.getMessage().contains("Failed to extract map entry value because of type mismatch "
                + "[expectedClass=java.lang.Long, actualClass=java.lang.String]"));
    }

    protected void checkParsingError(Supplier<HecatonInstance> clientSupplier) {
        instance1 = newHecatonInstance(true);

        createMapping(instance1, MAP_NAME, long.class, long.class);
        IMap<Long, Long> map = instance1.getMap(MAP_NAME);
        map.put(1L, 1L);

        HecatonInstance target = clientSupplier != null ? clientSupplier.get() : instance1;

        HecatonSqlException error = assertSqlException(target, new SqlStatement("SELECT bad_field FROM " + MAP_NAME));
        assertErrorCode(SqlErrorCode.PARSING, error);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    protected void checkUserCancel(Supplier<HecatonInstance> clientSupplier) {
        instance1 = newHecatonInstance(true);

        HecatonInstance target = clientSupplier != null ? clientSupplier.get() : instance1;

        try (SqlResult res = target.getSql().execute("select * from table(generate_stream(1))")) {
            sleepSeconds(1);
            res.close();

            try {
                for (SqlRow ignore : res) {
                    // No-op.
                }

                fail("Exception is not thrown");
            } catch (HecatonSqlException e) {
                assertErrorCode(SqlErrorCode.CANCELLED_BY_USER, e);
            }
        }
    }

    @Nonnull
    protected static HecatonSqlException assertSqlException(HecatonInstance instance, SqlStatement query) {
        try {
            execute(instance, query, false);

            fail("Exception is not thrown");

            return null;
        } catch (HecatonSqlException e) {
            System.out.println(">>> Caught expected SQL error: " + e);

            return e;
        }
    }

    @Nonnull
    protected static HecatonSqlException assertSqlExceptionWithShutdown(HecatonInstance instance, SqlStatement query) {
        try {
            execute(instance, query, true);

            fail("Exception is not thrown");

            return null;
        } catch (HecatonSqlException e) {
            System.out.println(">>> Caught expected SQL error: " + e);

            return e;
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    protected static int execute(HecatonInstance instance, SqlStatement query, boolean immediateShutdown) {
        try (SqlResult res = instance.getSql().execute(query)) {
            int count = 0;
            if (immediateShutdown) {
                instance.shutdown();
            }

            for (SqlRow ignore : res) {
                count++;
            }

            return count;
        }
    }

    protected static SqlStatement query() {
        return new SqlStatement("SELECT __key, this FROM " + MAP_NAME);
    }

    protected static void populate(HecatonInstance instance) {
        populate(instance, DATA_SET_SIZE);
    }

    protected static void populate(HecatonInstance instance, int size) {
        Map<Long, Long> map = new HashMap<>();

        for (long i = 0; i < size; i++) {
            map.put(i, i);
        }

        instance.getMap(MAP_NAME).putAll(map);
    }



    protected static void assertErrorCode(int expected, HecatonSqlException error) {
        assertEquals(error.getCode() + ": " + error.getMessage(), expected, error.getCode());
    }

    protected static Throwable findRootCause(Throwable t) {
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }

    /**
     * Start the new Hecaton instance.
     *
     * @param awaitAssignment whether to wait for a partition assignment to a new member
     * @return created instance
     */
    protected HecatonInstance newHecatonInstance(boolean awaitAssignment) {
        HecatonInstance instance = factory.newHecatonInstance(getConfig());

        if (awaitAssignment) {
            assertTrueEventually(() -> {
                Set<UUID> memberIds = new HashSet<>();

                for (Member member : instance.getCluster().getMembers()) {
                    memberIds.add(member.getUuid());
                }

                PartitionService partitionService = instance.getPartitionService();

                Set<UUID> assignedMemberIds = new HashSet<>();

                for (Partition partition : partitionService.getPartitions()) {
                    Member owner = partition.getOwner();

                    assertNotNull(owner);

                    assignedMemberIds.add(owner.getUuid());
                }

                assertEquals(memberIds, assignedMemberIds);
            });
        }

        return instance;
    }
}
