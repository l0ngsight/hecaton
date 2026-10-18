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

package uk.org.hecaton.jet.sql.impl.opt.physical;

import uk.org.hecaton.jet.sql.impl.connector.SqlConnectorCache;
import uk.org.hecaton.jet.sql.impl.connector.test.TestAbstractSqlConnector;
import uk.org.hecaton.jet.sql.impl.connector.test.TestStreamSqlConnector;
import uk.org.hecaton.jet.sql.impl.opt.OptimizerTestSupport;
import uk.org.hecaton.jet.sql.impl.schema.HecatonTable;
import uk.org.hecaton.jet.sql.impl.schema.HecatonTableStatistic;
import uk.org.hecaton.jet.sql.impl.schema.RelationsStorage;
import uk.org.hecaton.jet.sql.impl.schema.TableResolverImpl;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.sql.impl.schema.Table;
import uk.org.hecaton.sql.impl.schema.TableResolver;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static uk.org.hecaton.jet.impl.util.Util.getNodeEngine;
import static uk.org.hecaton.sql.impl.extract.QueryPath.KEY;
import static uk.org.hecaton.sql.impl.extract.QueryPath.VALUE;
import static uk.org.hecaton.sql.impl.type.QueryDataType.INT;
import static uk.org.hecaton.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static uk.org.hecaton.sql.impl.type.QueryDataTypeFamily.TIMESTAMP;
import static java.util.Arrays.asList;

public class PhysicalJoinTest extends OptimizerTestSupport {
    private TableResolver resolver;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Before
    public void setUp() throws Exception {
        NodeEngine nodeEngine = getNodeEngine(instance());
        resolver = new TableResolverImpl(
                nodeEngine,
                new RelationsStorage(nodeEngine),
                new SqlConnectorCache(nodeEngine));
    }

    @Test
    public void when_isSimpleJoin_then_useNestedLoopJoin() {
        HecatonTable tableLeft = partitionedTable("l", asList(field(KEY, INT), field(VALUE, INT)), 1);
        HecatonTable tableRight = partitionedTable("r", asList(field(KEY, INT), field(VALUE, INT)), 1);

        String query = "SELECT * FROM l JOIN r ON l.__key = r.__key";
        assertPlan(
                optimizePhysical(query, asList(INT, INT, INT, INT), tableLeft, tableRight).getPhysical(),
                plan(
                        planRow(0, JoinNestedLoopPhysicalRel.class),
                        planRow(1, FullScanPhysicalRel.class),
                        planRow(1, FullScanPhysicalRel.class)
                )
        );
    }

    @Test
    public void when_rightChildIsNotTableScan_then_useHashJoin() {
        HecatonTable tableLeft = partitionedTable("l", asList(field(KEY, INT), field(VALUE, INT)), 1);
        HecatonTable tableRight = partitionedTable("r", asList(field(KEY, INT), field(VALUE, INT)), 1);

        String query = "SELECT * FROM l WHERE EXISTS (SELECT 1 FROM r WHERE l.__key = r.__key)";
        assertPlan(
                optimizePhysical(query, asList(), tableLeft, tableRight).getPhysical(),
                plan(
                        planRow(0, CalcPhysicalRel.class),
                        planRow(1, JoinHashPhysicalRel.class),
                        planRow(2, FullScanPhysicalRel.class),
                        planRow(2, AggregateCombineByKeyPhysicalRel.class),
                        planRow(3, AggregateAccumulateByKeyPhysicalRel.class),
                        planRow(4, FullScanPhysicalRel.class)
                )
        );
    }

    @Ignore("Support streaming tables with watermarks in OptimizerTestSupport")
    @Test
    public void when_bothInputsAreStreamScan_then_useS2SJoin() {
        String leftStream = "l";
        TestStreamSqlConnector.create(
                instance().getSql(),
                leftStream,
                asList("a", "b"),
                asList(INTEGER, TIMESTAMP),
                row(1, timestamp(1L))
        );

        String rightStream = "r";
        TestStreamSqlConnector.create(
                instance().getSql(),
                rightStream,
                asList("x", "y"),
                asList(INTEGER, TIMESTAMP),
                row(1, timestamp(1L))
        );

        assertInstanceOf(TestAbstractSqlConnector.TestTable.class, resolver.getTables().get(0));
        assertInstanceOf(TestAbstractSqlConnector.TestTable.class, resolver.getTables().get(1));
        HecatonTable tableLeft = streamingTable(resolver.getTables().get(0));
        HecatonTable tableRight = streamingTable(resolver.getTables().get(1));

        String query = "SELECT * FROM l JOIN r ON l.b = r.y";
        assertPlan(
                optimizePhysical(query, List.of(), tableLeft, tableRight).getPhysical(),
                plan(
                        planRow(0, StreamToStreamJoinPhysicalRel.class),
                        planRow(1, FullScanPhysicalRel.class),
                        planRow(1, FullScanPhysicalRel.class)
                )
        );
    }

    private static HecatonTable streamingTable(Table table) {
        return new HecatonTable(table, new HecatonTableStatistic(1));
    }
}
