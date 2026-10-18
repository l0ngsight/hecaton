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

import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.jet.sql.impl.inject.PrimitiveUpsertTargetDescriptor;
import uk.org.hecaton.jet.sql.impl.inject.UpsertInjector;
import uk.org.hecaton.jet.sql.impl.inject.UpsertTarget;
import uk.org.hecaton.sql.impl.expression.CastExpression;
import uk.org.hecaton.sql.impl.expression.ColumnExpression;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;
import uk.org.hecaton.sql.impl.extract.QueryPath;
import uk.org.hecaton.sql.impl.row.JetSqlRow;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nullable;

import static uk.org.hecaton.jet.core.JetTestSupport.TEST_SS;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ProjectorTest {

    @Test
    public void test_project() {
        Projector projector = new Projector(
                new QueryPath[]{QueryPath.create("this.field")},
                new QueryDataType[]{QueryDataType.BIGINT},
                new MultiplyingTarget(),
                singletonList(CastExpression.create(ColumnExpression.create(0, QueryDataType.INT), QueryDataType.BIGINT)),
                mock(ExpressionEvalContext.class)
        );

        Object value = projector.project(new JetSqlRow(TEST_SS, new Object[]{1}));

        assertThat(value).isEqualTo(2L);
    }

    @Test
    public void test_supplierSerialization() {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

        Projector.Supplier original = Projector.supplier(
                new QueryPath[]{QueryPath.create("this.field")},
                new QueryDataType[]{QueryDataType.INT},
                PrimitiveUpsertTargetDescriptor.INSTANCE,
                singletonList(ColumnExpression.create(0, QueryDataType.INT))
        );

        Projector.Supplier serialized = serializationService.toObject(serializationService.toData(original));

        assertThat(serialized).isEqualToComparingFieldByField(original);
    }

    private static final class MultiplyingTarget implements UpsertTarget {

        private Object value;

        private MultiplyingTarget() {
            value = -1;
        }

        @Override
        public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
            return value -> this.value = value;
        }

        @Override
        public void init() {
            value = null;
        }

        @Override
        public Object conclude() {
            return (long) value * 2;
        }
    }
}
