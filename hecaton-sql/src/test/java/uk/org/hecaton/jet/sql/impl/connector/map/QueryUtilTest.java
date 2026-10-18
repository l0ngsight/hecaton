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

import uk.org.hecaton.instance.impl.TestUtil;
import uk.org.hecaton.internal.serialization.impl.AbstractSerializationService;
import uk.org.hecaton.jet.sql.SqlTestSupport;
import uk.org.hecaton.jet.sql.impl.connector.keyvalue.KvRowProjector;
import uk.org.hecaton.nio.serialization.DataSerializable;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.sql.impl.expression.ExpressionEvalContext;
import uk.org.hecaton.sql.impl.extract.QueryPath;
import uk.org.hecaton.sql.impl.row.JetSqlRow;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.jet.sql.impl.connector.map.QueryUtil.toPredicate;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryUtilTest extends SqlTestSupport {

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, null);
    }

    @Test
    public void when_leftValueIsNull_then_returnsNull() {
        Predicate<Object, Object> predicate =
                toPredicate(new JetSqlRow(TEST_SS, new Object[1]), new int[]{0}, new int[]{0}, new QueryPath[]{QueryPath.KEY_PATH});

        assertThat(predicate).isNull();
    }

    @Test
    public void when_serializedObject_then_deserializedCorrect() {
        AbstractSerializationService service = (AbstractSerializationService) TestUtil.getNode(instance()).getSerializationService();

        var evalContextMock = mock(ExpressionEvalContext.class);
        when(evalContextMock.getSerializationService()).thenReturn(mock());
        when(evalContextMock.getArguments()).thenReturn(emptyList());
        when(evalContextMock.getNodeEngine()).thenReturn(mock());

        var supplier = KvRowProjector.supplier(
                new QueryPath[]{},
                new QueryDataType[]{},
                null,
                null,
                null,
                null
        );
        DataSerializable projection = (DataSerializable) QueryUtil.toProjection(supplier, evalContextMock);

        var data = service.toData(projection);
        var actual = service.toObject(data);

        assertThat(actual)
                .usingRecursiveComparison()
                .comparingOnlyFields("arguments")
                .isEqualTo(projection);
    }
}
