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

package uk.org.hecaton.jet.sql.impl.processors;

import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.jet.config.JobConfig;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.core.test.TestSupport;
import uk.org.hecaton.jet.sql.SqlTestSupport;
import uk.org.hecaton.sql.impl.expression.Expression;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.jet.TestContextSupport.adaptSupplier;
import static uk.org.hecaton.jet.impl.JetServiceBackend.SQL_ARGUMENTS_KEY_NAME;
import static uk.org.hecaton.sql.impl.expression.ColumnExpression.create;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

@Category({QuickTest.class, ParallelJVMTest.class})
@RunWith(HecatonSerialClassRunner.class)
public class LateItemsDropPTest extends SqlTestSupport {
    private static final Expression<?> timestampEx = create(0, QueryDataType.BIGINT);

    @BeforeClass
    public static void beforeClass() {
        initialize(1, null);
    }

    @Test
    public void when_noEventIsLate_then_successful() {
        SupplierEx<Processor> supplier = () -> new LateItemsDropP((byte) 0, timestampEx);

        TestSupport.verifyProcessor(adaptSupplier(ProcessorSupplier.of(supplier)))
                .hecatonInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(SqlTestSupport::compareRowLists)
                .disableSnapshots()
                .input(asList(
                        wm(0L),
                        jetRow(0L, 1L),
                        jetRow(1L, 2L),
                        jetRow(2L, 2L),
                        wm(3L)
                ))
                .expectOutput(asList(
                        wm(0L),
                        jetRow(0L, 1L),
                        jetRow(1L, 2L),
                        jetRow(2L, 2L),
                        wm(3L)
                ));
    }

    @Test
    public void when_oneEventIsLate_then_dropEvent() {
        SupplierEx<Processor> supplier = () -> new LateItemsDropP((byte) 0, timestampEx);

        TestSupport.verifyProcessor(adaptSupplier(ProcessorSupplier.of(supplier)))
                .hecatonInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(SqlTestSupport::compareRowLists)
                .disableSnapshots()
                .input(asList(
                        wm(0L),
                        jetRow(0L, 1L),
                        jetRow(1L, 2L),
                        wm(3L),
                        jetRow(1L, 2L)
                ))
                .expectOutput(asList(
                        wm(0L),
                        jetRow(0L, 1L),
                        jetRow(1L, 2L),
                        wm(3L)
                ));
    }

    @Test
    public void when_fewEventsAreLate_then_dropEvents() {
        SupplierEx<Processor> supplier = () -> new LateItemsDropP((byte) 0, timestampEx);

        TestSupport.verifyProcessor(adaptSupplier(ProcessorSupplier.of(supplier)))
                .hecatonInstance(instance())
                .jobConfig(new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, emptyList()))
                .outputChecker(SqlTestSupport::compareRowLists)
                .disableSnapshots()
                .input(asList(
                        wm(0L),
                        jetRow(0L, 1L),
                        jetRow(1L, 2L),
                        wm(3L),
                        jetRow(2L, 2L),
                        jetRow(1L, 3L),
                        wm(5L)
                ))
                .expectOutput(asList(
                        wm(0L),
                        jetRow(0L, 1L),
                        jetRow(1L, 2L),
                        wm(3L),
                        wm(5L)
                ));
    }
}
