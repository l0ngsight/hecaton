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
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.core.test.TestSupport;
import uk.org.hecaton.jet.sql.SqlTestSupport;
import uk.org.hecaton.jet.sql.impl.connector.keyvalue.KvRowProjector;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.sql.impl.expression.ColumnExpression;
import uk.org.hecaton.sql.impl.expression.ConstantExpression;
import uk.org.hecaton.sql.impl.expression.Expression;
import uk.org.hecaton.sql.impl.extract.GenericQueryTargetDescriptor;
import uk.org.hecaton.sql.impl.extract.QueryPath;
import uk.org.hecaton.sql.impl.row.JetSqlRow;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static uk.org.hecaton.jet.TestContextSupport.adaptSupplier;
import static uk.org.hecaton.jet.impl.JetServiceBackend.SQL_ARGUMENTS_KEY_NAME;
import static uk.org.hecaton.sql.impl.type.QueryDataType.BOOLEAN;
import static uk.org.hecaton.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@RunWith(HecatonSerialClassRunner.class)
public class JoinByPrimitiveKeyProcessorTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";

    @SuppressWarnings("unchecked")
    private static final Expression<Boolean> TRUE_PREDICATE =
            (Expression<Boolean>) ConstantExpression.create(true, BOOLEAN);
    private static final Expression<?> PROJECTION = ColumnExpression.create(1, VARCHAR);
    @SuppressWarnings("unchecked")
    private static final Expression<Boolean> FALSE_PREDICATE =
            (Expression<Boolean>) ConstantExpression.create(false, BOOLEAN);

    private IMap<Object, Object> map;

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
    }

    @Before
    public void before() {
        map = instance().getMap(MAP_NAME);
    }

    @Test
    public void when_innerJoinLeftKeyIsNull_then_emptyResult() {
        runTest(TRUE_PREDICATE, PROJECTION, TRUE_PREDICATE, true,
                singletonList(jetRow((Object) null)),
                emptyList());
    }

    @Test
    public void when_innerJoinRightValueIsNull_then_emptyResult() {
        runTest(TRUE_PREDICATE, PROJECTION, TRUE_PREDICATE, true,
                singletonList(jetRow(1)),
                emptyList());
    }

    @Test
    public void when_innerJoinFilteredOutByProjector_then_emptyResult() {
        map.put(1, "value");
        runTest(FALSE_PREDICATE, PROJECTION, TRUE_PREDICATE, true,
                singletonList(jetRow(1)),
                emptyList());
    }

    @Test
    public void when_innerJoinProjectedByProjector_then_modified() {
        map.put(1, "value");
        runTest(TRUE_PREDICATE, ConstantExpression.create("modified", VARCHAR), TRUE_PREDICATE, true,
                singletonList(jetRow(1)),
                singletonList(jetRow(1, "modified")));
    }

    @Test
    public void when_innerJoinFilteredOutByCondition_then_absent() {
        map.put(1, "value-1");
        runTest(TRUE_PREDICATE, PROJECTION, FALSE_PREDICATE, true,
                singletonList(jetRow(1)),
                emptyList());
    }

    @Test
    public void when_outerJoinLeftKeyIsNull_then_nulls() {
        runTest(TRUE_PREDICATE, PROJECTION, TRUE_PREDICATE, false,
                singletonList(jetRow((Object) null)),
                singletonList(jetRow(null, null)));
    }

    @Test
    public void when_outerJoinRightValueIsNull_then_nulls() {
        runTest(TRUE_PREDICATE, PROJECTION, TRUE_PREDICATE, false,
                singletonList(jetRow(1)),
                singletonList(jetRow(1, null)));
    }

    @Test
    public void when_outerJoinFilteredOutByProjector_then_nulls() {
        map.put(1, "value");
        runTest(FALSE_PREDICATE, PROJECTION, TRUE_PREDICATE, false,
                singletonList(jetRow(1)),
                singletonList(jetRow(1, null)));
    }

    @Test
    public void when_outerJoinProjectedByProjector_then_modified() {
        map.put(1, "value");
        runTest(TRUE_PREDICATE, ConstantExpression.create("modified", VARCHAR), TRUE_PREDICATE, false,
                singletonList(jetRow(1)),
                singletonList(jetRow(1, "modified")));
    }

    @Test
    public void when_outerJoinFilteredOutByCondition_then_nulls() {
        map.put(1, "value-1");
        runTest(TRUE_PREDICATE, PROJECTION, FALSE_PREDICATE, false,
                singletonList(jetRow(1)),
                singletonList(jetRow(1, null)));
    }

    private void runTest(
            Expression<Boolean> rowProjectorCondition,
            Expression<?> rowProjectorProjection,
            Expression<Boolean> joinCondition,
            boolean inner,
            List<JetSqlRow> input,
            List<JetSqlRow> output
    ) {
        KvRowProjector.Supplier projectorSupplier = KvRowProjector.supplier(
                new QueryPath[]{QueryPath.KEY_PATH, QueryPath.VALUE_PATH},
                new QueryDataType[]{QueryDataType.INT, VARCHAR},
                GenericQueryTargetDescriptor.DEFAULT,
                GenericQueryTargetDescriptor.DEFAULT,
                rowProjectorCondition,
                singletonList(rowProjectorProjection)
        );

        ProcessorSupplier processor =
                new JoinByPrimitiveKeyProcessorSupplier(inner, 0, joinCondition, MAP_NAME, projectorSupplier);

        TestSupport
                .verifyProcessor(adaptSupplier(processor))
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .input(input)
                .hecatonInstance(instance())
                .outputChecker(SqlTestSupport::compareRowLists)
                .disableProgressAssertion()
                .expectOutput(output);
    }
}
