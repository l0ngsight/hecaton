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

package uk.org.hecaton.jet.sql.impl.schema;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.LifecycleService;
import uk.org.hecaton.jet.sql.impl.connector.SqlConnector;
import uk.org.hecaton.jet.sql.impl.connector.SqlConnector.SqlExternalResource;
import uk.org.hecaton.jet.sql.impl.connector.SqlConnectorCache;
import uk.org.hecaton.mock.MockUtil;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.sql.impl.QueryException;
import uk.org.hecaton.sql.impl.schema.Mapping;
import uk.org.hecaton.sql.impl.schema.MappingField;
import uk.org.hecaton.sql.impl.schema.TableResolver.TableListener;
import uk.org.hecaton.sql.impl.schema.view.View;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import static uk.org.hecaton.sql.impl.type.QueryDataType.INT;
import static uk.org.hecaton.sql.impl.type.QueryDataType.OBJECT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TableResolverImplTest {

    private TableResolverImpl catalog;

    @Mock
    private NodeEngine nodeEngine;

    @Mock
    private RelationsStorage relationsStorage;

    @Mock
    private SqlConnectorCache connectorCache;

    @Mock
    private SqlConnector connector;

    @Mock
    private TableListener listener;

    @Mock
    private HecatonInstance hecatonInstance;

    @Mock
    private LifecycleService lifecycleService;

    private AutoCloseable openMocks;

    @Before
    public void before() {
        openMocks = openMocks(this);

        when(nodeEngine.getHecatonInstance()).thenReturn(hecatonInstance);
        when(hecatonInstance.getLifecycleService()).thenReturn(lifecycleService);
        catalog = new TableResolverImpl(nodeEngine, relationsStorage, connectorCache);
        catalog.registerListener(listener);
    }

    @After
    public void cleanUp() {
        MockUtil.closeMocks(openMocks);
    }

    // region mapping storage tests

    @Test
    public void when_createsInvalidMapping_then_throws() {
        // given
        Mapping mapping = mapping();

        given(connectorCache.forType(mapping.connectorType())).willReturn(connector);
        given(connector.typeName()).willReturn(mapping.connectorType());
        given(connector.defaultObjectType()).willReturn("Dummy");
        given(connector.resolveAndValidateFields(nodeEngine,
                new SqlExternalResource(mapping.externalName(), mapping.dataConnection(), mapping.connectorType(), "Dummy", mapping.options()),
                mapping.fields()
        ))
                .willThrow(new RuntimeException("expected test exception"));

        // when
        // then
        assertThatThrownBy(() -> catalog.createMapping(mapping, true, true, null))
                .hasMessageContaining("expected test exception");
        verify(relationsStorage, never()).putIfAbsent(anyString(), (Mapping) any());
        verify(relationsStorage, never()).put(anyString(), (Mapping) any());
        verifyNoInteractions(listener);
    }

    @Test
    public void when_createsDuplicateMapping_then_throws() {
        // given
        Mapping mapping = mapping();

        given(connectorCache.forType(mapping.connectorType())).willReturn(connector);
        given(connector.resolveAndValidateFields(nodeEngine,
                new SqlExternalResource(mapping.externalName(), mapping.dataConnection(), "Dummy", null, mapping.options()),
                mapping.fields()
        ))
                .willReturn(singletonList(new MappingField("field_name", INT)));
        given(connector.typeName()).willReturn("Dummy");
        given(connector.defaultObjectType()).willReturn("Dummy");
        given(relationsStorage.putIfAbsent(eq(mapping.name()), isA(Mapping.class))).willReturn(false);

        // when
        // then
        assertThatThrownBy(() -> catalog.createMapping(mapping, false, false, null))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Mapping or view already exists: name");
        verifyNoInteractions(listener);
    }

    @Test
    public void when_createsDuplicateMappingWithIfNotExists_then_succeeds() {
        // given
        Mapping mapping = mapping();

        given(connectorCache.forType(mapping.connectorType())).willReturn(connector);
        given(connector.typeName()).willReturn(mapping.connectorType());
        given(connector.defaultObjectType()).willReturn("Dummy");
        given(connector.resolveAndValidateFields(nodeEngine,
                new SqlExternalResource(mapping.externalName(), mapping.dataConnection(), mapping.connectorType(), null, mapping.options()),
                mapping.fields()
        ))
                .willReturn(singletonList(new MappingField("field_name", INT)));
        given(relationsStorage.putIfAbsent(eq(mapping.name()), isA(Mapping.class))).willReturn(false);

        // when
        catalog.createMapping(mapping, false, true, null);

        // then
        verifyNoInteractions(listener);
    }

    @Test
    public void when_replacesMapping_then_succeeds() {
        // given
        Mapping mapping = mapping();

        given(connectorCache.forType(mapping.connectorType())).willReturn(connector);
        given(connector.typeName()).willReturn(mapping.connectorType());
        given(connector.defaultObjectType()).willReturn("Dummy");
        given(connector.resolveAndValidateFields(nodeEngine,
                new SqlExternalResource(mapping.externalName(), mapping.dataConnection(), mapping.connectorType(), null, mapping.options()),
                mapping.fields()
        ))
                .willReturn(singletonList(new MappingField("field_name", INT)));

        // when
        catalog.createMapping(mapping, true, false, null);

        // then
        verify(relationsStorage).put(eq(mapping.name()), isA(Mapping.class));
        verify(listener).onTableChanged();
    }

    @Test
    public void when_mappingWithNoObjectType_then_usesDefault() {
        // given
        Mapping mapping = mapping();

        given(connectorCache.forType(mapping.connectorType())).willReturn(connector);
        given(connector.resolveAndValidateFields(nodeEngine,
                new SqlExternalResource(mapping.externalName(), mapping.dataConnection(), mapping.connectorType(), "MyDummyType", mapping.options()),
                mapping.fields()
        ))
                .willReturn(singletonList(new MappingField("field_name", INT)));
        // in case of mistake, throw error:
        given(connector.resolveAndValidateFields(nodeEngine,
                new SqlExternalResource(mapping.externalName(), mapping.dataConnection(), mapping.connectorType(), null, mapping.options()),
                mapping.fields()
        ))
                .willThrow(new AssertionError("Object type must not be null"));
        given(connector.typeName()).willReturn(mapping.connectorType());
        given(connector.defaultObjectType()).willReturn("MyDummyType");

        // when
        catalog.createMapping(mapping, true, false, null);

        // then
        verify(relationsStorage).put(eq(mapping.name()), isA(Mapping.class));
        verify(listener).onTableChanged();
    }

    @Test
    public void when_removesExistingMapping_then_callsListeners() {
        // given
        String name = "name";

        given(relationsStorage.removeMapping(name)).willReturn(mapping());

        // when
        // then
        catalog.removeMapping(name, false);
        verify(listener).onTableChanged();
    }

    @Test
    public void when_removesNonExistingMapping_then_throws() {
        // given
        String name = "name";

        given(relationsStorage.removeMapping(name)).willReturn(null);

        // when
        // then
        assertThatThrownBy(() -> catalog.removeMapping(name, false))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Mapping does not exist: name");
        verifyNoInteractions(listener);
    }

    @Test
    public void when_removesNonExistingMappingWithIfExists_then_succeeds() {
        // given
        String name = "name";

        given(relationsStorage.removeMapping(name)).willReturn(null);

        // when
        // then
        catalog.removeMapping(name, true);
        verifyNoInteractions(listener);
    }

    // endregion

    // region view storage tests

    @Test
    public void when_createsView_then_succeeds() {
        // given
        View view = view();
        given(relationsStorage.putIfAbsent(view.name(), view)).willReturn(true);

        // when
        catalog.createView(view, false, false);

        // then
        verify(relationsStorage).putIfAbsent(eq(view.name()), isA(View.class));
    }

    @Test
    public void when_createsViewIfNotExists_then_succeeds() {
        // given
        View view = view();
        given(relationsStorage.putIfAbsent(view.name(), view)).willReturn(true);

        // when
        catalog.createView(view, false, true);

        // then
        verify(relationsStorage).putIfAbsent(eq(view.name()), isA(View.class));
    }

    @Test
    public void when_createsDuplicateViewsIfReplace_then_succeeds() {
        // given
        View view = view();

        // when
        catalog.createView(view, true, false);

        // then
        verify(relationsStorage).put(eq(view.name()), isA(View.class));
    }

    @Test
    public void when_createsDuplicateViewsIfReplaceAndIfNotExists_then_succeeds() {
        // given
        View view = view();

        // when
        catalog.createView(view, true, true);

        // then
        verify(relationsStorage).putIfAbsent(eq(view.name()), isA(View.class));
    }

    @Test
    public void when_createsDuplicateViews_then_throws() {
        // given
        View view = view();
        given(relationsStorage.putIfAbsent(eq(view.name()), isA(View.class))).willReturn(false);

        // when
        // then
        assertThatThrownBy(() -> catalog.createView(view, false, false))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("Mapping or view already exists: name");
        verifyNoInteractions(listener);
    }

    @Test
    public void when_removesNonExistingView_then_throws() {
        // given
        String name = "name";

        given(relationsStorage.removeView(name)).willReturn(null);

        // when
        // then
        assertThatThrownBy(() -> catalog.removeView(name, false))
                .isInstanceOf(QueryException.class)
                .hasMessageContaining("View does not exist: name");
        verifyNoInteractions(listener);
    }

    @Test
    public void when_removesNonExistingViewWithIfExists_then_succeeds() {
        // given
        String name = "name";

        given(relationsStorage.removeView(name)).willReturn(null);

        // when
        // then
        catalog.removeView(name, true);
        verifyNoInteractions(listener);
    }

    // endregion

    private static Mapping mapping() {
        return new Mapping("name", "external_name", null, "type", null, emptyList(), emptyMap());
    }

    private static View view() {
        return new View("name", "SELECT * FROM map", singletonList("*"), asList(OBJECT, OBJECT));
    }
}
