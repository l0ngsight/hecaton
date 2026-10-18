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
import uk.org.hecaton.jet.core.ProcessorMetaSupplier;
import uk.org.hecaton.jet.core.test.TestSupport;
import uk.org.hecaton.jet.sql.SqlTestSupport;
import uk.org.hecaton.jet.sql.impl.JetJoinInfo;
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
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static uk.org.hecaton.jet.TestContextSupport.adaptSupplier;
import static uk.org.hecaton.jet.impl.JetServiceBackend.SQL_ARGUMENTS_KEY_NAME;
import static uk.org.hecaton.sql.impl.type.QueryDataType.BOOLEAN;
import static uk.org.hecaton.sql.impl.type.QueryDataType.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;

@RunWith(HecatonSerialClassRunner.class)
public class JoinByEquiJoinProcessorTest extends SqlTestSupport {

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
        initialize(1, null);
    }

    @Before
    public void before() {
        map = instance().getMap(MAP_NAME);
    }

    @Test
    public void test_innerJoin() {
        map.put(1, "value");
        runTest(INNER, TRUE_PREDICATE, PROJECTION, TRUE_PREDICATE,
                singletonList(jetRow(1)),
                singletonList(jetRow(1, "value")));
    }

    @Test
    public void when_innerJoinFilteredOutByProjector_then_absent() {
        map.put(1, "value");
        runTest(INNER, FALSE_PREDICATE, PROJECTION, TRUE_PREDICATE,
                singletonList(jetRow(1)),
                emptyList());
    }

    @Test
    public void when_innerJoinProjectedByProjector_then_modified() {
        map.put(1, "value");
        runTest(INNER, TRUE_PREDICATE, ConstantExpression.create("modified", VARCHAR), TRUE_PREDICATE,
                singletonList(jetRow(1)),
                singletonList(jetRow(1, "modified")));
    }

    @Test
    public void when_innerJoinFilteredOutByCondition_then_absent() {
        map.put(1, "value");
        runTest(INNER, TRUE_PREDICATE, PROJECTION, FALSE_PREDICATE,
                singletonList(jetRow(1)),
                emptyList());
    }

    @Test
    public void test_outerJoin() {
        map.put(1, "value");
        runTest(LEFT, TRUE_PREDICATE, PROJECTION, TRUE_PREDICATE,
                asList(jetRow(1), jetRow(2)),
                asList(jetRow(1, "value"), jetRow(2, null)));
    }

    @Test
    public void when_outerJoinFilteredOutByProjector_then_absent() {
        map.put(1, "value");
        runTest(LEFT, FALSE_PREDICATE, PROJECTION, TRUE_PREDICATE,
                asList(jetRow(1), jetRow(2)),
                asList(jetRow(1, null), jetRow(2, null)));
    }

    @Test
    public void when_outerJoinProjectedByProjector_then_modified() {
        map.put(1, "value");
        runTest(LEFT, TRUE_PREDICATE, ConstantExpression.create("modified", VARCHAR), TRUE_PREDICATE,
                asList(jetRow(1), jetRow(2)),
                asList(jetRow(1, "modified"), jetRow(2, null)));
    }

    @Test
    public void when_outerJoinFilteredOutByCondition_then_absent() {
        map.put(1, "value");
        runTest(LEFT, TRUE_PREDICATE, PROJECTION, FALSE_PREDICATE,
                asList(jetRow(1), jetRow(2)),
                asList(jetRow(1, null), jetRow(2, null)));
    }

    private void runTest(
            JoinRelType joinType,
            Expression<Boolean> rowProjectorCondition,
            Expression<?> rowProjectorProjection,
            Expression<Boolean> nonEquiCondition,
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

        ProcessorMetaSupplier processor = JoinByEquiJoinProcessorSupplier.supplier(
                new JetJoinInfo(joinType, new int[]{0}, new int[]{0}, nonEquiCondition, null),
                MAP_NAME,
                projectorSupplier
        );

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
