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

package uk.org.hecaton.multimap.impl;

import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.EntryListener;
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.map.IMapEvent;
import uk.org.hecaton.map.MapEvent;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.map.impl.DataAwareEntryEvent;
import uk.org.hecaton.map.impl.event.EntryEventData;
import uk.org.hecaton.map.impl.event.EventData;
import uk.org.hecaton.map.impl.event.MapEventData;

/**
 * Dispatches multiMapEvents to appropriate methods of given listener
 */
class MultiMapEventsDispatcher {

    private final ILogger logger = Logger.getLogger(MultiMapEventsDispatcher.class);

    private final ClusterService clusterService;
    private final MultiMapService multiMapService;

    MultiMapEventsDispatcher(MultiMapService multiMapService, ClusterService clusterService) {
        this.multiMapService = multiMapService;
        this.clusterService = clusterService;
    }

    private void incrementEventStats(IMapEvent event) {
        multiMapService.getLocalMultiMapStatsImpl(event.getName()).incrementReceivedEvents();
    }

    public void dispatchEvent(EventData eventData, EntryListener listener) {
        if (eventData instanceof EntryEventData) {
            dispatchEntryEventData(eventData, listener);
        } else if (eventData instanceof MapEventData) {
            dispatchMapEventData(eventData, listener);
        } else {
            throw new IllegalArgumentException("Unknown multimap event data");
        }
    }

    private void dispatchMapEventData(EventData eventData, EntryListener listener) {
        MapEventData mapEventData = (MapEventData) eventData;
        Member member = getMemberOrNull(eventData);
        if (member == null) {
            return;
        }
        MapEvent event = createMapEvent(mapEventData, member);
        dispatch0(event, listener);
        incrementEventStats(event);
    }

    private MapEvent createMapEvent(MapEventData mapEventData, Member member) {
        return new MapEvent(mapEventData.getMapName(), member,
                mapEventData.getEventType(), mapEventData.getNumberOfEntries());
    }

    private void dispatchEntryEventData(EventData eventData, EntryListener listener) {
        EntryEventData entryEventData = (EntryEventData) eventData;
        Member member = getMemberOrNull(eventData);

        EntryEvent event = createDataAwareEntryEvent(entryEventData, member);
        dispatch0(event, listener);
        incrementEventStats(event);
    }

    private Member getMemberOrNull(EventData eventData) {
        Member member = clusterService.getMember(eventData.getCaller());
        if (member == null) {
            if (logger.isInfoEnabled()) {
                logger.info("Dropping event " + eventData + " from unknown address:" + eventData.getCaller());
            }
        }
        return member;
    }

    private DataAwareEntryEvent createDataAwareEntryEvent(EntryEventData entryEventData, Member member) {
        return new DataAwareEntryEvent(member, entryEventData.getEventType(), entryEventData.getMapName(),
                entryEventData.getDataKey(), entryEventData.getDataNewValue(),
                entryEventData.getDataOldValue(), entryEventData.getDataMergingValue(),
                multiMapService.getSerializationService());
    }

    private void dispatch0(IMapEvent event, EntryListener listener) {
        String namespace = MultiMapService.lookupNamespace(multiMapService.getNodeEngine(), event.getName());
        NamespaceUtil.setupNamespace(multiMapService.getNodeEngine(), namespace);
        try {
            switch (event.getEventType()) {
                case ADDED:
                    listener.entryAdded((EntryEvent) event);
                    break;
                case REMOVED:
                    listener.entryRemoved((EntryEvent) event);
                    break;
                case CLEAR_ALL:
                    listener.mapCleared((MapEvent) event);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid event type: " + event.getEventType());
            }
        } finally {
            NamespaceUtil.cleanupNamespace(multiMapService.getNodeEngine(), namespace);
        }
    }
}
