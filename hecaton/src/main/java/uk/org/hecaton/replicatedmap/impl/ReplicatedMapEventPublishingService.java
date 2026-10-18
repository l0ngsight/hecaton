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

package uk.org.hecaton.replicatedmap.impl;

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.cluster.impl.MemberImpl;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ReplicatedMapConfig;
import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.EntryEventType;
import uk.org.hecaton.core.EntryListener;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.map.MapEvent;
import uk.org.hecaton.map.impl.DataAwareEntryEvent;
import uk.org.hecaton.map.impl.event.EntryEventData;
import uk.org.hecaton.map.impl.event.EventData;
import uk.org.hecaton.map.impl.event.MapEventData;
import uk.org.hecaton.internal.monitor.impl.LocalReplicatedMapStatsImpl;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.query.impl.QueryEntry;
import uk.org.hecaton.query.impl.getters.Extractors;
import uk.org.hecaton.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;
import uk.org.hecaton.replicatedmap.impl.record.AbstractReplicatedRecordStore;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedQueryEventFilter;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedRecordStore;
import uk.org.hecaton.spi.impl.eventservice.EventFilter;
import uk.org.hecaton.spi.impl.eventservice.EventPublishingService;
import uk.org.hecaton.spi.impl.eventservice.EventRegistration;
import uk.org.hecaton.spi.impl.eventservice.EventService;
import uk.org.hecaton.spi.impl.NodeEngine;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.EventListener;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.Future;

import static uk.org.hecaton.core.EntryEventType.ADDED;
import static uk.org.hecaton.core.EntryEventType.REMOVED;
import static uk.org.hecaton.core.EntryEventType.UPDATED;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;
import static uk.org.hecaton.replicatedmap.impl.ReplicatedMapService.SERVICE_NAME;

/**
 * Dispatches published events on replicated map to corresponding listeners.
 */
public class ReplicatedMapEventPublishingService
        implements EventPublishingService {

    private final HashMap<String, Boolean> statisticsMap = new HashMap<>();

    private final ReplicatedMapService replicatedMapService;
    private final NodeEngine nodeEngine;
    private final Config config;
    private final EventService eventService;
    private final InternalSerializationService serializationService;
    private final Extractors extractors;

    public ReplicatedMapEventPublishingService(ReplicatedMapService replicatedMapService) {
        this.replicatedMapService = replicatedMapService;
        this.nodeEngine = replicatedMapService.getNodeEngine();
        this.config = nodeEngine.getConfig();
        this.eventService = nodeEngine.getEventService();
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.extractors = Extractors.newBuilder(this.serializationService).build();
    }

    @Override
    public void dispatchEvent(Object event, Object listener) {
        if ((event instanceof EntryEventData entryEventData)) {
            Member member = getMember(entryEventData);
            EntryEvent entryEvent = createDataAwareEntryEvent(entryEventData, member);
            EntryListener entryListener = (EntryListener) listener;

            runWithNamespaceAwareness(entryEventData.getMapName(), () -> {
                switch (entryEvent.getEventType()) {
                    case ADDED:
                        entryListener.entryAdded(entryEvent);
                        break;
                    case EVICTED:
                        entryListener.entryEvicted(entryEvent);
                        break;
                    case UPDATED:
                        entryListener.entryUpdated(entryEvent);
                        break;
                    case REMOVED:
                        entryListener.entryRemoved(entryEvent);
                        break;
                    default:
                        throw new IllegalArgumentException("event type " + entryEvent.getEventType() + " not supported");
                }
            });

            String mapName = entryEventData.getMapName();
            Boolean statisticsEnabled = statisticsMap.computeIfAbsent(mapName, x -> {
                ReplicatedMapConfig mapConfig = config.findReplicatedMapConfig(mapName);
                return mapConfig.isStatisticsEnabled();
            });
            if (statisticsEnabled) {
                int partitionId = nodeEngine.getPartitionService().getPartitionId(entryEventData.getDataKey());
                ReplicatedRecordStore recordStore = replicatedMapService.getPartitionContainer(partitionId)
                                                                        .getRecordStore(mapName);
                if (recordStore instanceof AbstractReplicatedRecordStore store) {
                    LocalReplicatedMapStatsImpl stats = store.getStats();
                    stats.incrementReceivedEvents();
                }
            }
        } else if (event instanceof MapEventData mapEventData) {
            Member member = getMember(mapEventData);
            MapEvent mapEvent = new MapEvent(mapEventData.getMapName(), member,
                    mapEventData.getEventType(), mapEventData.getNumberOfEntries());
            EntryListener entryListener = (EntryListener) listener;
            EntryEventType type = EntryEventType.getByType(mapEventData.getEventType());
            if (type == EntryEventType.CLEAR_ALL) {
                runWithNamespaceAwareness(mapEventData.getMapName(), () -> entryListener.mapCleared(mapEvent));
            } else {
                throw new IllegalArgumentException("Unsupported EntryEventType: " + type);
            }
        }
    }

    private void runWithNamespaceAwareness(String mapName, Runnable runnable) {
        NamespaceUtil.runWithNamespace(nodeEngine, ReplicatedMapService.lookupNamespace(nodeEngine, mapName), runnable);
    }

    public @Nonnull
    UUID addLocalEventListener(EventListener entryListener, EventFilter eventFilter, String mapName) {
        if (nodeEngine.getLocalMember().isLiteMember()) {
            throw new ReplicatedMapCantBeCreatedOnLiteMemberException(nodeEngine.getThisAddress());
        }
        if (entryListener instanceof HecatonInstanceAware aware) {
            aware.setHecatonInstance(nodeEngine.getHecatonInstance());
        }
        EventRegistration registration = eventService.registerLocalListener(SERVICE_NAME, mapName, eventFilter,
                entryListener);
        return registration.getId();
    }

    public boolean removeEventListener(@Nonnull String mapName, @Nonnull UUID registrationId) {
        checkNotNull(registrationId, "registrationId cannot be null");
        if (nodeEngine.getLocalMember().isLiteMember()) {
            throw new ReplicatedMapCantBeCreatedOnLiteMemberException(nodeEngine.getThisAddress());
        }
        return eventService.deregisterListener(SERVICE_NAME, mapName, registrationId);
    }

    public Future<Boolean> removeEventListenerAsync(@Nonnull String mapName, @Nonnull UUID registrationId) {
        checkNotNull(registrationId, "registrationId cannot be null");
        if (nodeEngine.getLocalMember().isLiteMember()) {
            throw new ReplicatedMapCantBeCreatedOnLiteMemberException(nodeEngine.getThisAddress());
        }
        return eventService.deregisterListenerAsync(SERVICE_NAME, mapName, registrationId);
    }

    public void fireMapClearedEvent(int deletedEntrySize, String name) {
        EventService eventService = nodeEngine.getEventService();
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, name);
        if (registrations.isEmpty()) {
            return;
        }
        MapEventData mapEventData = new MapEventData(name, name, nodeEngine.getThisAddress(),
                EntryEventType.CLEAR_ALL.getType(), deletedEntrySize);
        eventService.publishEvent(SERVICE_NAME, registrations, mapEventData, name.hashCode());
    }

    private Member getMember(EventData eventData) {
        Member member = replicatedMapService.getNodeEngine().getClusterService().getMember(eventData.getCaller());
        if (member == null) {
            member = new MemberImpl.Builder(eventData.getCaller())
                    .version(nodeEngine.getVersion())
                    .build();
        }
        return member;
    }

    private DataAwareEntryEvent createDataAwareEntryEvent(EntryEventData entryEventData, Member member) {
        return new DataAwareEntryEvent(member, entryEventData.getEventType(), entryEventData.getMapName(),
                entryEventData.getDataKey(), entryEventData.getDataNewValue(), entryEventData.getDataOldValue(),
                entryEventData.getDataMergingValue(), nodeEngine.getSerializationService());
    }

    public void fireEntryListenerEvent(Data key, Data oldValue, Data value, String name, Address caller) {
        EntryEventType eventType = value == null ? REMOVED : oldValue == null ? ADDED : UPDATED;
        fireEntryListenerEvent(key, oldValue, value, eventType, name, caller);
    }

    public void fireEntryListenerEvent(Data key, Data oldValue, Data value, EntryEventType eventType, String name,
                                       Address caller) {
        Collection<EventRegistration> registrations = eventService.getRegistrations(SERVICE_NAME, name);
        if (registrations.isEmpty()) {
            return;
        }
        EntryEventData eventData = new EntryEventData(name, name, caller, key, value, oldValue, eventType.getType());
        for (EventRegistration registration : registrations) {
            if (!shouldPublish(key, oldValue, value, eventType, registration.getFilter())) {
                continue;
            }
            eventService.publishEvent(SERVICE_NAME, registration, eventData, key.hashCode());
        }
    }

    private boolean shouldPublish(Data key, Data oldValue, Data value, EntryEventType eventType, EventFilter filter) {
        QueryEntry queryEntry = null;
        if (filter instanceof ReplicatedQueryEventFilter) {
            Data testValue;
            if (eventType == REMOVED) {
                testValue = oldValue;
            } else {
                testValue = value;
            }
            queryEntry = new QueryEntry(serializationService, key, testValue, extractors);
        }
        return filter == null || filter.eval(queryEntry != null ? queryEntry : key);
    }
}
