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

import uk.org.hecaton.jet.core.DAG;
import uk.org.hecaton.jet.core.ProcessorMetaSupplier;
import uk.org.hecaton.jet.core.Vertex;
import uk.org.hecaton.jet.sql.impl.JetJoinInfo;
import uk.org.hecaton.jet.sql.impl.connector.SqlConnector.VertexWithInputConfig;
import uk.org.hecaton.jet.sql.impl.connector.keyvalue.KvRowProjector;
import uk.org.hecaton.mock.MockUtil;
import uk.org.hecaton.sql.impl.extract.QueryPath;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import static uk.org.hecaton.sql.impl.extract.QueryPath.KEY_PATH;
import static uk.org.hecaton.sql.impl.extract.QueryPath.VALUE_PATH;
import static org.apache.calcite.rel.core.JoinRelType.INNER;
import static org.apache.calcite.rel.core.JoinRelType.LEFT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.BDDMockito.given;
import static org.mockito.MockitoAnnotations.openMocks;

@RunWith(JUnitParamsRunner.class)
public class JoinerTest {

    @Mock
    private DAG dag;

    @Mock
    private Vertex vertex;

    @Mock
    private KvRowProjector.Supplier rightRowProjectorSupplier;

    private AutoCloseable openMocks;

    @Before
    public void setUp() {
        openMocks = openMocks(this);
    }

    @After
    public void cleanUp() {
        MockUtil.closeMocks(openMocks);
    }

    @SuppressWarnings("unused")
    private Object[] joinTypes() {
        return new Object[]{INNER, LEFT};
    }

    @Test
    @Parameters(method = "joinTypes")
    public void test_joinByPrimitiveKey(JoinRelType joinType) {
        // given
        given(rightRowProjectorSupplier.paths()).willReturn(new QueryPath[]{KEY_PATH});
        given(dag.newUniqueVertex(contains("Lookup"), isA(JoinByPrimitiveKeyProcessorSupplier.class))).willReturn(vertex);

        // when
        VertexWithInputConfig vertexWithConfig = Joiner.join(
                dag,
                "imap-name",
                "table-name",
                joinInfo(joinType, new int[]{0}, new int[]{0}),
                rightRowProjectorSupplier
        );

        // then
        assertThat(vertexWithConfig.vertex()).isEqualTo(vertex);
        assertThat(vertexWithConfig.configureEdgeFn()).isNotNull();
    }

    @Test
    @Parameters(method = "joinTypes")
    public void test_joinByPredicate(JoinRelType joinType) {
        // given
        given(rightRowProjectorSupplier.paths()).willReturn(new QueryPath[]{QueryPath.create("path")});
        given(dag.newUniqueVertex(contains("Predicate"), isA(ProcessorMetaSupplier.class))).willReturn(vertex);

        // when
        VertexWithInputConfig vertexWithConfig = Joiner.join(
                dag,
                "imap-name",
                "table-name",
                joinInfo(joinType, new int[]{0}, new int[]{0}),
                rightRowProjectorSupplier
        );

        // then
        assertThat(vertexWithConfig.vertex()).isEqualTo(vertex);
        assertThat(vertexWithConfig.configureEdgeFn()).isNotNull();
    }

    @Test
    @Parameters(method = "joinTypes")
    public void test_joinByScan(JoinRelType joinType) {
        // given
        given(rightRowProjectorSupplier.paths()).willReturn(new QueryPath[]{VALUE_PATH});
        given(dag.newUniqueVertex(contains("Scan"), isA(JoinScanProcessorSupplier.class))).willReturn(vertex);

        // when
        VertexWithInputConfig vertexWithConfig = Joiner.join(
                dag,
                "imap-name",
                "table-name",
                joinInfo(joinType, new int[0], new int[0]),
                rightRowProjectorSupplier
        );

        // then
        assertThat(vertexWithConfig.vertex()).isEqualTo(vertex);
        assertThat(vertexWithConfig.configureEdgeFn()).isNull();
    }

    private static JetJoinInfo joinInfo(JoinRelType joinType, int[] leftEquiJoinIndices, int[] rightEquiJoinIndices) {
        return new JetJoinInfo(joinType, leftEquiJoinIndices, rightEquiJoinIndices, null, null);
    }
}
