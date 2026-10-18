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

package uk.org.hecaton.client.map.impl.querycache.subscriber;

import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.ContinuousQueryAddListenerCodec;
import uk.org.hecaton.client.impl.protocol.codec.MapRemoveEntryListenerCodec;
import uk.org.hecaton.client.impl.spi.ClientListenerService;
import uk.org.hecaton.client.impl.spi.EventHandler;
import uk.org.hecaton.client.impl.spi.impl.ListenerMessageCodec;
import uk.org.hecaton.client.impl.spi.impl.listener.ClientListenerServiceImpl;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.util.ConstructorFunction;
import uk.org.hecaton.internal.util.executor.StripedExecutor;
import uk.org.hecaton.internal.util.executor.StripedRunnable;
import uk.org.hecaton.internal.util.executor.TimeoutRunnable;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.map.EventLostEvent;
import uk.org.hecaton.map.IMapEvent;
import uk.org.hecaton.map.impl.ListenerAdapter;
import uk.org.hecaton.map.impl.event.EventData;
import uk.org.hecaton.map.impl.querycache.QueryCacheEventService;
import uk.org.hecaton.map.impl.querycache.event.BatchEventData;
import uk.org.hecaton.map.impl.querycache.event.BatchIMapEvent;
import uk.org.hecaton.map.impl.querycache.event.LocalEntryEventData;
import uk.org.hecaton.map.impl.querycache.event.QueryCacheEventData;
import uk.org.hecaton.map.impl.querycache.event.SingleIMapEvent;
import uk.org.hecaton.map.listener.MapListener;
import uk.org.hecaton.query.impl.QueryEntry;
import uk.org.hecaton.query.impl.getters.Extractors;
import uk.org.hecaton.spi.impl.eventservice.EventFilter;
import uk.org.hecaton.spi.impl.eventservice.impl.TrueEventFilter;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static uk.org.hecaton.internal.util.Preconditions.checkHasText;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;
import static uk.org.hecaton.map.impl.querycache.subscriber.EventPublisherHelper.createIMapEvent;
import static uk.org.hecaton.map.impl.querycache.subscriber.QueryCacheEventListenerAdapters.createQueryCacheListenerAdaptor;

/**
 * Client side event service implementation for query cache.
 *
 * @see QueryCacheEventService
 */
public class ClientQueryCacheEventService implements QueryCacheEventService {

    private static final int EVENT_QUEUE_TIMEOUT_MILLIS = 500;

    private static final ConstructorFunction<String, QueryCacheToListenerMapper> REGISTRY_CONSTRUCTOR =
            new ConstructorFunction<>() {
                @Override
                public QueryCacheToListenerMapper createNew(String arg) {
                    return new QueryCacheToListenerMapper();
                }
            };

    private final StripedExecutor executor;
    private final ClientListenerService listenerService;
    private final InternalSerializationService serializationService;
    private final ILogger logger = Logger.getLogger(getClass());
    private final ConcurrentMap<String, QueryCacheToListenerMapper> registrations;

    public ClientQueryCacheEventService(HecatonClientInstanceImpl client) {
        ClientListenerServiceImpl listenerService = (ClientListenerServiceImpl) client.getListenerService();
        this.listenerService = listenerService;
        this.serializationService = client.getSerializationService();
        this.executor = listenerService.getEventExecutor();
        this.registrations = new ConcurrentHashMap<>();
    }

    @Override
    public boolean hasListener(String mapName, String cacheId) {
        QueryCacheToListenerMapper queryCacheToListenerMapper = registrations.get(mapName);
        if (queryCacheToListenerMapper == null) {
            return false;
        }

        return queryCacheToListenerMapper.hasListener(cacheId);
    }

    // used for testing purposes
    public ConcurrentMap<String, QueryCacheToListenerMapper> getRegistrations() {
        return registrations;
    }

    @Override
    public void sendEventToSubscriber(String name, Object eventData, int orderKey) {
        // this is already subscriber side. So no need to implement it for subscriber side.
        throw new UnsupportedOperationException();
    }

    @Override
    public void publish(String mapName, String cacheId, Object event,
                        int orderKey, Extractors extractors) {
        checkHasText(mapName, "mapName");
        checkHasText(cacheId, "cacheId");
        checkNotNull(event, "event cannot be null");


        Collection<ListenerInfo> listeners = getListeners(mapName, cacheId);
        for (ListenerInfo info : listeners) {
            if (!canPassFilter(event, info.getFilter(), extractors)) {
                continue;
            }

            try {
                executor.execute(new EventDispatcher(event, info, orderKey,
                        serializationService, EVENT_QUEUE_TIMEOUT_MILLIS));
            } catch (RejectedExecutionException e) {
                // TODO Should we notify user when we overloaded?
                logger.warning("EventQueue overloaded! Can not process IMap=[" + mapName + "]"
                        + ", QueryCache=[ " + cacheId + "]" + ", Event=[" + event + "]");
            }
        }
    }

    private boolean canPassFilter(Object eventData,
                                  EventFilter filter, Extractors extractors) {
        if (filter == null || filter instanceof TrueEventFilter) {
            return true;
        }

        if (!(eventData instanceof LocalEntryEventData)) {
            return true;
        }

        LocalEntryEventData localEntryEventData = (LocalEntryEventData) eventData;

        if (localEntryEventData.getEventType() != EventLostEvent.EVENT_TYPE) {
            Object value = getValueOrOldValue(localEntryEventData);
            Data keyData = localEntryEventData.getKeyData();
            QueryEntry entry = new QueryEntry(serializationService, keyData, value, extractors);
            return filter.eval(entry);
        }

        return true;
    }

    private Object getValueOrOldValue(LocalEntryEventData localEntryEventData) {
        Object value = localEntryEventData.getValue();
        return value != null ? value : localEntryEventData.getOldValue();
    }

    @Override
    public UUID addPublisherListener(String mapName, String cacheId, ListenerAdapter adapter) {
        EventHandler handler = new QueryCacheHandler(adapter);
        return listenerService.registerListener(createPublisherListenerCodec(cacheId), handler);
    }

    @Override
    public boolean removePublisherListener(String mapName, String cacheId, UUID listenerId) {
        return listenerService.deregisterListener(listenerId);
    }

    private ListenerMessageCodec createPublisherListenerCodec(final String listenerName) {
        return new ListenerMessageCodec() {
            @Override
            public ClientMessage encodeAddRequest(boolean localOnly) {
                return ContinuousQueryAddListenerCodec.encodeRequest(listenerName, localOnly);
            }

            @Override
            public UUID decodeAddResponse(ClientMessage clientMessage) {
                return ContinuousQueryAddListenerCodec.decodeResponse(clientMessage);
            }

            @Override
            public ClientMessage encodeRemoveRequest(UUID realRegistrationId) {
                return MapRemoveEntryListenerCodec.encodeRequest(listenerName, realRegistrationId);
            }

            @Override
            public boolean decodeRemoveResponse(ClientMessage clientMessage) {
                return MapRemoveEntryListenerCodec.decodeResponse(clientMessage);
            }

        };
    }

    @Override
    public UUID addListener(String mapName, String cacheId, MapListener listener) {
        return addListener(mapName, cacheId, listener, null);
    }

    @Override
    public UUID addListener(String mapName, String cacheId, MapListener listener, EventFilter filter) {
        checkHasText(mapName, "mapName");
        checkHasText(cacheId, "cacheId");
        checkNotNull(listener, "listener cannot be null");

        QueryCacheToListenerMapper queryCacheToListenerMapper = getOrPutIfAbsent(registrations, mapName, REGISTRY_CONSTRUCTOR);
        ListenerAdapter listenerAdaptor = createQueryCacheListenerAdaptor(listener);
        return queryCacheToListenerMapper.addListener(cacheId, listenerAdaptor, filter);
    }

    @Override
    public boolean removeListener(String mapName, String cacheId, UUID listenerId) {
        checkHasText(mapName, "mapName");
        checkHasText(cacheId, "cacheId");
        checkNotNull(listenerId, "listenerId cannot be null");

        QueryCacheToListenerMapper queryCacheToListenerMapper = getOrPutIfAbsent(registrations, mapName, REGISTRY_CONSTRUCTOR);
        return queryCacheToListenerMapper.removeListener(cacheId, listenerId);
    }

    @Override
    public void removeAllListeners(String mapName, String cacheId) {
        checkHasText(mapName, "mapName");
        checkHasText(cacheId, "cacheId");

        QueryCacheToListenerMapper queryCacheToListenerMap = registrations.get(mapName);
        if (queryCacheToListenerMap != null) {
            queryCacheToListenerMap.removeAllListeners(cacheId);
        }
    }

    /**
     * Query cache event handler.
     */
    private final class QueryCacheHandler extends ContinuousQueryAddListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {
        private final ListenerAdapter adapter;

        private QueryCacheHandler(ListenerAdapter adapter) {
            this.adapter = adapter;
        }

        @Override
        public void handleQueryCacheSingleEvent(QueryCacheEventData data) {
            adapter.onEvent(new SingleIMapEvent(data));
        }

        @Override
        public void handleQueryCacheBatchEvent(Collection<QueryCacheEventData> events, String source, int partitionId) {
            adapter.onEvent(new BatchIMapEvent(new BatchEventData(events, source, partitionId)));
        }
    }

    private Collection<ListenerInfo> getListeners(String mapName, String cacheName) {
        QueryCacheToListenerMapper queryCacheToListenerMapper = registrations.get(mapName);
        if (queryCacheToListenerMapper == null) {
            return Collections.emptySet();
        }
        return queryCacheToListenerMapper.getListenerInfos(cacheName);
    }

    /**
     * Dispatches an event to a listener.
     */
    private static class EventDispatcher implements StripedRunnable, TimeoutRunnable {

        private final Object event;
        private final ListenerInfo listenerInfo;
        private final int orderKey;
        private final long timeoutMs;
        private final SerializationService serializationService;

        EventDispatcher(Object event, ListenerInfo listenerInfo, int orderKey,
                        SerializationService serializationService, long timeoutMs) {
            this.event = event;
            this.listenerInfo = listenerInfo;
            this.orderKey = orderKey;
            this.timeoutMs = timeoutMs;
            this.serializationService = serializationService;
        }

        @Override
        public int getKey() {
            return orderKey;
        }

        @Override
        public void run() {
            EventData eventData = (EventData) event;
            EventFilter filter = listenerInfo.getFilter();

            IMapEvent event = createIMapEvent(eventData, filter, null, serializationService);
            ListenerAdapter listenerAdapter = listenerInfo.getListenerAdapter();
            listenerAdapter.onEvent(event);
        }

        @Override
        public long getTimeout() {
            return timeoutMs;
        }

        @Override
        public TimeUnit getTimeUnit() {
            return TimeUnit.MILLISECONDS;
        }

    }
}
