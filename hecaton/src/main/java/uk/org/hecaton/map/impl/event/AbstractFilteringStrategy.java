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

import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.map.impl.EntryEventFilter;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.query.QueryEventFilter;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.query.impl.CachedQueryEntry;
import uk.org.hecaton.query.impl.QueryableEntry;
import uk.org.hecaton.query.impl.getters.Extractors;
import uk.org.hecaton.spi.impl.eventservice.EventFilter;

/**
 * A common abstract class to support implementations of filtering strategies.
 *
 * @see DefaultEntryEventFilteringStrategy
 * @see QueryCacheNaturalFilteringStrategy
 */
public abstract class AbstractFilteringStrategy implements FilteringStrategy {

    protected final InternalSerializationService serializationService;
    protected final MapServiceContext mapServiceContext;

    protected AbstractFilteringStrategy(InternalSerializationService serializationService,
                                     MapServiceContext mapServiceContext) {
        this.serializationService = serializationService;
        this.mapServiceContext = mapServiceContext;
    }

    protected String getThisNodesAddress() {
        return mapServiceContext.getNodeEngine().getThisAddress().toString();
    }

    /**
     * Evaluates filters of {@link EntryEventFilter} type.
     *
     * @param filter  the filter which must be a {@link EntryEventFilter}
     * @param dataKey the event entry key
     * @return {@code true} if the filter matches
     */
    protected boolean processEntryEventFilter(EventFilter filter, Data dataKey) {
        EntryEventFilter eventFilter = (EntryEventFilter) filter;
        return eventFilter.eval(dataKey);
    }

    /**
     * Evaluates the {@code filter} using a {@link CachedQueryEntry} together with the
     * value {@link Extractors} configured for this map. The filter must be of {@link QueryEventFilter} type.
     *
     * @param filter        a {@link QueryEventFilter} filter
     * @param dataKey       the entry key
     * @param testValue     the value used to evaluate the filter
     * @param mapNameOrNull the map name. May be null if this is not a map event (e.g. cache event)
     * @return {@code true} if the entry matches the query event filter
     */
    @SuppressWarnings("rawtypes")
    protected boolean evaluateQueryEventFilter(EventFilter filter, Data dataKey, Object testValue, String mapNameOrNull) {
        Extractors extractors = getExtractorsForMapName(mapNameOrNull);
        QueryEventFilter queryEventFilter = (QueryEventFilter) filter;
        QueryableEntry entry = new CachedQueryEntry(serializationService,
                dataKey, testValue, extractors);
        if (mapNameOrNull == null) {
            return queryEventFilter.eval(entry);
        } else {
            MapContainer mapContainer = mapServiceContext.getMapContainer(mapNameOrNull);
            return NamespaceUtil.callWithNamespace(mapContainer.getMapConfig().getUserCodeNamespace(),
                    () -> queryEventFilter.eval(entry));
        }
    }

    /**
     * Returns the value {@link Extractors} for the map with the given name. May be null in
     * which case no extractors are returned.
     */
    private Extractors getExtractorsForMapName(String mapNameOrNull) {
        if (mapNameOrNull == null) {
            return Extractors.newBuilder(serializationService)
                    .build();
        }
        return mapServiceContext.getExtractors(mapNameOrNull);
    }
}
