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

package uk.org.hecaton.jet.sql.impl.connector.map;

import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.core.test.TestSupport;
import uk.org.hecaton.jet.impl.memory.AccumulationLimitExceededException;
import uk.org.hecaton.jet.sql.SqlTestSupport;
import uk.org.hecaton.jet.sql.impl.connector.keyvalue.KvProjector;
import uk.org.hecaton.jet.sql.impl.inject.PrimitiveUpsertTargetDescriptor;
import uk.org.hecaton.sql.impl.QueryException;
import uk.org.hecaton.sql.impl.extract.QueryPath;
import uk.org.hecaton.sql.impl.row.JetSqlRow;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

import static uk.org.hecaton.jet.TestContextSupport.adaptSupplier;
import static uk.org.hecaton.jet.impl.JetServiceBackend.SQL_ARGUMENTS_KEY_NAME;
import static uk.org.hecaton.query.impl.predicates.PredicateTestUtils.entry;
import static uk.org.hecaton.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonSerialClassRunner.class)
public class InsertProcessorTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";

    private Map<Object, Object> map;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Before
    public void before() {
        map = instance().getMap(MAP_NAME);
    }

    @Test
    public void test_insert() {
        executeInsert(singletonList(jetRow(1, 1)), singletonList(jetRow(1L)));
        assertThat(map).containsExactly(entry(1, 1));
    }

    @Test
    public void test_multiInsert() {
        executeInsert(asList(jetRow(1, 1), jetRow(2, 2)), singletonList(jetRow(2L)));
        assertThat(map).containsExactlyInAnyOrderEntriesOf(Map.of(1, 1, 2, 2));
    }

    @Test
    public void test_noInput() {
        executeInsert(emptyList(), singletonList(jetRow(0L)));
        assertThat(map).isEmpty();
    }

    @Test
    public void when_keyIsNull_then_fail() {
        assertThatThrownBy(() -> executeInsert(singletonList(jetRow(null, 1)), emptyList()))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Cannot write NULL to '__key' field");
    }

    @Test
    public void when_ValueIsNull_then_fail() {
        assertThatThrownBy(() -> executeInsert(singletonList(jetRow(1, null)), emptyList()))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Cannot write NULL to 'this' field");
    }

    @Test
    public void when_keyAlreadyExists_then_fail() {
        assertThatThrownBy(() -> executeInsert(() -> map.put(1, 1), singletonList(jetRow(1, 2)), emptyList()))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Duplicate key");
    }

    @Test
    public void when_attemptsToInsertDuplicateKey_then_fail() {
        assertThatThrownBy(() -> executeInsert(asList(jetRow(1, 1), jetRow(1, 2)), emptyList()))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Duplicate key");
    }

    @Test
    public void when_maxAccumulatedKeysCountIsExceeded_then_fail() {
        assertThatThrownBy(() -> executeInsert(1, asList(jetRow(1, 1), jetRow(2, 2)), emptyList()))
                .isInstanceOf(AccumulationLimitExceededException.class);
    }

    private void executeInsert(List<JetSqlRow> rows, List<JetSqlRow> expectedOutput) {
        executeInsert(() -> map.clear(), Long.MAX_VALUE, rows, expectedOutput);
    }

    private void executeInsert(long maxAccumulatedKeys, List<JetSqlRow> rows, List<JetSqlRow> expectedOutput) {
        executeInsert(() -> map.clear(), maxAccumulatedKeys, rows, expectedOutput);
    }

    private void executeInsert(Runnable setup, List<JetSqlRow> rows, List<JetSqlRow> expectedOutput) {
        executeInsert(setup, Long.MAX_VALUE, rows, expectedOutput);
    }

    private void executeInsert(Runnable setup, long maxAccumulatedKeys, List<JetSqlRow> rows, List<JetSqlRow> expectedOutput) {
        KvProjector.Supplier projectorSupplier = KvProjector.supplier(
                new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH},
                new QueryDataType[]{INT, INT},
                PrimitiveUpsertTargetDescriptor.INSTANCE,
                PrimitiveUpsertTargetDescriptor.INSTANCE,
                true
        );

        InsertProcessorSupplier processor = new InsertProcessorSupplier(MAP_NAME, projectorSupplier);

        JobConfig config = new JobConfig()
                .setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList())
                .setMaxProcessorAccumulatedRecords(maxAccumulatedKeys);
        TestSupport
                .verifyProcessor(adaptSupplier(processor))
                .hecatonInstance(instance())
                .jobConfig(config)
                .disableSnapshots()
                .disableProgressAssertion()
                .executeBeforeEachRun(setup)
                .input(rows)
                .outputChecker(SqlTestSupport::compareRowLists)
                .expectOutput(expectedOutput);
    }
}
