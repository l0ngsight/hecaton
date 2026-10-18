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

package uk.org.hecaton.jet.sql.impl.connector;

import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.sql.impl.expression.ColumnExpression;
import uk.org.hecaton.sql.impl.expression.ConstantExpression;
import uk.org.hecaton.sql.impl.expression.Expression;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;
import uk.org.hecaton.sql.impl.expression.math.MultiplyFunction;
import uk.org.hecaton.sql.impl.extract.QueryExtractor;
import uk.org.hecaton.sql.impl.extract.QueryTarget;
import uk.org.hecaton.sql.impl.row.JetSqlRow;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.sql.impl.type.QueryDataType.BOOLEAN;
import static uk.org.hecaton.sql.impl.type.QueryDataType.INT;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RowProjectorTest {

    @Test
    public void test_project() {
        RowProjector projector = new RowProjector(
                new String[]{"target"},
                new QueryDataType[]{INT},
                new IdentityTarget(),
                null,
                singletonList(
                        MultiplyFunction.create(ColumnExpression.create(0, INT), ConstantExpression.create(2, INT), INT)
                ),
                mock(ExpressionEvalContext.class)
        );

        JetSqlRow row = projector.project(1);

        assertThat(row.getValues()).isEqualTo(new Object[]{2});
    }

    @Test
    @SuppressWarnings("unchecked")
    public void when_filteredByPredicate_then_returnsNull() {
        RowProjector projector = new RowProjector(
                new String[]{"target"},
                new QueryDataType[]{INT},
                new IdentityTarget(),
                (Expression<Boolean>) ConstantExpression.create(Boolean.FALSE, BOOLEAN),
                emptyList(),
                mock(ExpressionEvalContext.class)
        );

        JetSqlRow row = projector.project(1);

        assertThat(row).isNull();
    }

    private static final class IdentityTarget implements QueryTarget {

        private Object value;

        private IdentityTarget() {
        }

        @Override
        public void setTarget(Object value, Data valueData) {
            assert valueData == null;
            this.value = value;
        }

        @Override
        public QueryExtractor createExtractor(String path, QueryDataType type) {
            return () -> value;
        }
    }
}
