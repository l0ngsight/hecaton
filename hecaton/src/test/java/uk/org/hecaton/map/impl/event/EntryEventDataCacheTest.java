/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.map.impl.event;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.core.EntryEventType;
import uk.org.hecaton.internal.serialization.impl.HeapData;
import uk.org.hecaton.map.impl.EventListenerFilter;
import uk.org.hecaton.map.impl.ListenerAdapter;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.listener.EntryAddedListener;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.eventservice.impl.TrueEventFilter;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

import static uk.org.hecaton.core.EntryEventType.INVALIDATION;
import static uk.org.hecaton.map.impl.ListenerAdapters.createListenerAdapter;
import static uk.org.hecaton.map.impl.MapListenerFlagOperator.setAndGetListenerFlags;
import static uk.org.hecaton.map.impl.event.FilteringStrategy.FILTER_DOES_NOT_MATCH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test EntryEventDataCache implementations
 */
@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EntryEventDataCacheTest {

    private EntryEventDataCache instance;

    private static final Address ADDRESS;

    static {
        Address address = null;
        try {
            address = new Address("127.0.0.1", 5701);
        } catch (UnknownHostException e) {
        }
        ADDRESS = address;
    }

    @Parameterized.Parameter
    public FilteringStrategy filteringStrategy;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> parameters() {
        // setup mock MapServiceContext & NodeEngine, required by FilteringStrategy's
        MapServiceContext mapServiceContext = mock(MapServiceContext.class);
        NodeEngine mockNodeEngine = mock(NodeEngine.class);
        when(mockNodeEngine.getThisAddress()).thenReturn(ADDRESS);
        when(mapServiceContext.toData(any())).thenReturn(new HeapData());
        when(mapServiceContext.getNodeEngine()).thenReturn(mockNodeEngine);

        return Arrays.asList(new Object[][]{
                {new DefaultEntryEventFilteringStrategy(null, mapServiceContext)},
                {new QueryCacheNaturalFilteringStrategy(null, mapServiceContext)},
        });
    }

    @Before
    public void setup() {
        instance = filteringStrategy.getEntryEventDataCache();
    }

    @Test
    public void getOrCreateEventDataIncludingValues_whenAlreadyCached() {
        // when: creating EntryEventData including values with same arguments
        EntryEventData eed = instance.getOrCreateEventData("test", ADDRESS, new HeapData(), new Object(),
                new Object(), new Object(), EntryEventType.ADDED.getType(), true);

        EntryEventData shouldBeCached = instance.getOrCreateEventData("test", ADDRESS, new HeapData(), new Object(),
                new Object(), new Object(), EntryEventType.ADDED.getType(), true);

        // then: returned instances are the same
        assertSame(eed, shouldBeCached);
    }

    @Test
    public void getOrCreateEventDataExcludingValues_whenAlreadyCached() {
        // when: creating EntryEventData including values with same arguments
        EntryEventData eed = instance.getOrCreateEventData("test", ADDRESS, new HeapData(), new Object(),
                new Object(), new Object(), EntryEventType.ADDED.getType(), false);

        EntryEventData shouldBeCached = instance.getOrCreateEventData("test", ADDRESS, new HeapData(), new Object(),
                new Object(), new Object(), EntryEventType.ADDED.getType(), false);

        // then: returned instances are the same
        assertSame(eed, shouldBeCached);
    }

    @Test
    public void isEmpty_whenNoEntryEventDataHaveBeenCreated() {
        // when: no EntryEventData have been getOrCreate'd
        // then: the cache is empty
        assertTrue(instance.isEmpty());
    }

    @Test
    public void isEmpty_whenEntryEventDataHaveBeenCreated() {
        // when: EntryEventData have been getOrCreate'd
        instance.getOrCreateEventData("test", ADDRESS, new HeapData(), new Object(),
                new Object(), new Object(), EntryEventType.ADDED.getType(), false);

        // then: the cache is not empty
        assertFalse(instance.isEmpty());
    }

    @Test
    public void eventDataIncludingValues_whenValueIsCached() {
        // when: EntryEventData including values have been created
        EntryEventData eed = instance.getOrCreateEventData("test", ADDRESS, new HeapData(), new Object(),
                new Object(), new Object(), EntryEventType.ADDED.getType(), true);

        // then: the cache is not empty & eventDataIncludingValues returns the cached entry
        assertFalse(instance.isEmpty());
        assertSame(eed, instance.eventDataIncludingValues().iterator().next());
    }

    @Test
    public void eventDataIncludingValues_whenNoValuesCached() {
        // when: EntryEventData including values have been created

        // then: the cache is empty & eventDataIncludingValues returns null or empty collection
        assertTrue(instance.isEmpty());
        assertTrue(instance.eventDataIncludingValues() == null || instance.eventDataIncludingValues().isEmpty());
    }

    @Test
    public void eventDataIncludingValues_whenDataExcludingValuesAreCached() {
        // when: EntryEventData excluding values have been created
        EntryEventData eed = instance.getOrCreateEventData("test", ADDRESS, new HeapData(), new Object(),
                new Object(), new Object(), EntryEventType.ADDED.getType(), false);

        // then: eventDataIncludingValues returns null or empty collection
        assertTrue(instance.eventDataIncludingValues() == null || instance.eventDataIncludingValues().isEmpty());
    }

    @Test
    public void eventDataExcludingValues_whenValueIsCached() {
        // when: EntryEventData excluding values have been created
        EntryEventData eed = instance.getOrCreateEventData("test", ADDRESS, new HeapData(), new Object(),
                new Object(), new Object(), EntryEventType.ADDED.getType(), false);

        // then: the cache is not empty & eventDataExcludingValues returns the cached entry
        assertFalse(instance.isEmpty());
        assertSame(eed, instance.eventDataExcludingValues().iterator().next());
    }

    @Test
    public void eventDataExcludingValues_whenNoValuesCached() {
        // when: no EntryEventData values have been created

        // then: the cache is empty & eventDataIncludingValues returns null or empty collection
        assertTrue(instance.isEmpty());
        assertTrue(instance.eventDataIncludingValues() == null || instance.eventDataIncludingValues().isEmpty());
    }

    @Test
    public void eventDataExcludingValues_whenDataIncludingValuesAreCached() {
        // when: no EntryEventData values have been created
        EntryEventData eed = instance.getOrCreateEventData("test", ADDRESS, new HeapData(), new Object(),
                new Object(), new Object(), EntryEventType.ADDED.getType(), true);

        // then: the cache is empty & eventDataIncludingValues returns null or empty collection
        assertTrue(instance.eventDataExcludingValues() == null || instance.eventDataExcludingValues().isEmpty());
    }

    @Test
    public void filteringStrategy_rejects_invalidation_events() {
        EventListenerFilter filter = createInvalidationEventRejectingFilter();
        int matched = filteringStrategy.doFilter(filter, null, null, null, INVALIDATION, "mapName");

        assertEquals(FILTER_DOES_NOT_MATCH, matched);
    }

    private static EventListenerFilter createInvalidationEventRejectingFilter() {
        ListenerAdapter<Object> listenerAdapter = createListenerAdapter((EntryAddedListener<Object, Object>) event -> {

        });

        int flags = setAndGetListenerFlags(listenerAdapter);
        return new EventListenerFilter(flags, TrueEventFilter.INSTANCE);
    }
}
