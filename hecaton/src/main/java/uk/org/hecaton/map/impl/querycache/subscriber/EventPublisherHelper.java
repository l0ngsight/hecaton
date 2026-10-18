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

package uk.org.hecaton.map.impl.querycache.subscriber;

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.core.EntryEventType;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.map.EventLostEvent;
import uk.org.hecaton.map.IMapEvent;
import uk.org.hecaton.map.MapEvent;
import uk.org.hecaton.map.QueryCache;
import uk.org.hecaton.map.impl.DataAwareEntryEvent;
import uk.org.hecaton.map.impl.EntryEventFilter;
import uk.org.hecaton.map.impl.event.EventData;
import uk.org.hecaton.map.impl.querycache.QueryCacheContext;
import uk.org.hecaton.map.impl.querycache.QueryCacheEventService;
import uk.org.hecaton.map.impl.querycache.event.LocalCacheWideEventData;
import uk.org.hecaton.map.impl.querycache.event.LocalEntryEventData;
import uk.org.hecaton.map.impl.querycache.subscriber.record.QueryCacheRecord;
import uk.org.hecaton.query.impl.getters.Extractors;
import uk.org.hecaton.spi.impl.eventservice.EventFilter;

/**
 * Helper class for publishing events to {@code QueryCache} listeners.
 */
public final class EventPublisherHelper {

    private EventPublisherHelper() {
    }

    /**
     * Publishes event upon a change on a key in {@code QueryCache}.
     *
     * @param mapName       the name of underlying map.
     * @param cacheId       the name of {@code QueryCache}
     * @param queryCacheKey the key in {@code Data} format.
     * @param dataNewValue  the value in {@code Data} format.
     * @param oldRecord     the relevant {@code QueryCacheEntry}
     * @param context       the {@code QueryCacheContext}
     */
    static void publishEntryEvent(QueryCacheContext context, String mapName,
                                  String cacheId, Object queryCacheKey, Data dataNewValue,
                                  QueryCacheRecord oldRecord, EntryEventType eventType,
                                  Extractors extractors) {
        if (!hasListener(context, mapName, cacheId)) {
            return;
        }

        Data dataKey = context.getSerializationService().toData(queryCacheKey);

        QueryCacheEventService eventService = getQueryCacheEventService(context);

        Object oldValue = getOldValue(oldRecord);

        LocalEntryEventData eventData = createLocalEntryEventData(cacheId, dataKey, dataNewValue, oldValue,
                eventType.getType(), -1, context, mapName);
        eventService.publish(mapName, cacheId, eventData, dataKey.hashCode(), extractors);
    }

    public static boolean hasListener(QueryCache queryCache) {
        DefaultQueryCache defaultQueryCache = (DefaultQueryCache) queryCache;

        QueryCacheEventService eventService = getQueryCacheEventService(defaultQueryCache.context);
        return eventService.hasListener(defaultQueryCache.mapName, defaultQueryCache.cacheId);
    }

    private static boolean hasListener(QueryCacheContext context, String mapName, String cacheId) {
        QueryCacheEventService eventService = getQueryCacheEventService(context);
        return eventService.hasListener(mapName, cacheId);
    }

    /**
     * As a result of map-wide events like {@link EntryEventType#CLEAR_ALL} or {@link EntryEventType#EVICT_ALL}
     * we also publish a matching event for query-cache listeners.
     */
    static void publishCacheWideEvent(InternalQueryCache queryCache,
                                      int numberOfEntriesAffected,
                                      EntryEventType eventType) {
        if (!hasListener(queryCache)) {
            return;
        }

        DefaultQueryCache defaultQueryCache = (DefaultQueryCache) queryCache;
        QueryCacheContext context = defaultQueryCache.context;
        String mapName = defaultQueryCache.mapName;
        String cacheId = defaultQueryCache.cacheId;

        QueryCacheEventService eventService = getQueryCacheEventService(context);

        LocalCacheWideEventData eventData
                = new LocalCacheWideEventData(cacheId, eventType.getType(), numberOfEntriesAffected, mapName);

        eventService.publish(mapName, cacheId, eventData, cacheId.hashCode(), queryCache.getExtractors());
    }

    private static Object getOldValue(QueryCacheRecord oldRecord) {
        return oldRecord == null ? null : oldRecord.getValue();
    }

    private static LocalEntryEventData createLocalEntryEventData(String cacheId, Data dataKey, Data dataNewValue,
                                                                 Object oldValue, int eventType,
                                                                 int partitionId, QueryCacheContext context, String mapName) {
        SerializationService serializationService = context.getSerializationService();
        return new LocalEntryEventData(serializationService, cacheId, eventType, dataKey, oldValue, dataNewValue,
                partitionId, mapName);
    }

    private static QueryCacheEventService getQueryCacheEventService(QueryCacheContext context) {
        SubscriberContext subscriberContext = context.getSubscriberContext();
        return subscriberContext.getEventService();
    }

    public static void publishEventLost(QueryCacheContext context, String mapName,
                                        String cacheId, int partitionId, Extractors extractors) {
        QueryCacheEventService eventService = getQueryCacheEventService(context);
        int orderKey = cacheId.hashCode();

        eventService.publish(mapName, cacheId,
                createLocalEntryEventData(cacheId, null, null, null,
                        EventLostEvent.EVENT_TYPE, partitionId, context, mapName), orderKey, extractors);
    }

    public static IMapEvent createIMapEvent(EventData eventData, EventFilter filter, Member member,
                                            SerializationService serializationService) {
        String source = eventData.getSource();
        int eventType = eventData.getEventType();

        if (eventType == EventLostEvent.EVENT_TYPE) {
            LocalEntryEventData localEventData = (LocalEntryEventData) eventData;
            int partitionId = localEventData.getPartitionId();
            return new EventLostEvent(source, null, partitionId);
        }

        if (eventType == EntryEventType.CLEAR_ALL.getType() || eventType == EntryEventType.EVICT_ALL.getType()) {
            LocalCacheWideEventData localCacheWideEventData = (LocalCacheWideEventData) eventData;
            int numberOfEntriesAffected = localCacheWideEventData.getNumberOfEntriesAffected();
            return new MapEvent(source, null, eventType, numberOfEntriesAffected);
        }

        LocalEntryEventData localEntryEventData = (LocalEntryEventData) eventData;
        Data dataKey = localEntryEventData.getKeyData();
        Data dataNewValue = localEntryEventData.getValueData();
        Data dataOldValue = localEntryEventData.getOldValueData();
        boolean includeValue = isIncludeValue(filter);
        return new DataAwareEntryEvent(member, eventType, source, dataKey,
                (includeValue ? dataNewValue : null),
                (includeValue ? dataOldValue : null),
                null,
                serializationService);
    }

    private static boolean isIncludeValue(EventFilter filter) {
        if (filter instanceof EntryEventFilter eventFilter) {
            return eventFilter.isIncludeValue();
        }
        return true;
    }
}
