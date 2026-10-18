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

package uk.org.hecaton.map.impl;

import uk.org.hecaton.core.EntryEvent;
import uk.org.hecaton.core.EntryEventType;
import uk.org.hecaton.map.IMapEvent;
import uk.org.hecaton.map.MapEvent;
import uk.org.hecaton.internal.nearcache.impl.invalidation.Invalidation;
import uk.org.hecaton.map.impl.nearcache.invalidation.InvalidationListener;
import uk.org.hecaton.map.listener.EntryAddedListener;
import uk.org.hecaton.map.listener.EntryEvictedListener;
import uk.org.hecaton.map.listener.EntryExpiredListener;
import uk.org.hecaton.map.listener.EntryLoadedListener;
import uk.org.hecaton.map.listener.EntryMergedListener;
import uk.org.hecaton.map.listener.EntryRemovedListener;
import uk.org.hecaton.map.listener.EntryUpdatedListener;
import uk.org.hecaton.map.listener.MapClearedListener;
import uk.org.hecaton.map.listener.MapEvictedListener;
import uk.org.hecaton.map.listener.MapListener;
import uk.org.hecaton.internal.util.ConstructorFunction;

import java.util.EnumMap;
import java.util.Map;

/**
 * A static factory class which creates various
 * {@link uk.org.hecaton.map.impl.ListenerAdapter} implementations.
 */
public final class MapListenerAdaptors {

    /**
     * Converts an {@link uk.org.hecaton.map.listener.EntryAddedListener} to a {@link uk.org.hecaton.map.impl.ListenerAdapter}.
     */
    private static final ConstructorFunction<MapListener, ListenerAdapter> ENTRY_ADDED_LISTENER_ADAPTER_CONSTRUCTOR =
            mapListener -> {
                if (!(mapListener instanceof EntryAddedListener)) {
                    return null;
                }
                final EntryAddedListener listener = (EntryAddedListener) mapListener;
                return (ListenerAdapter<IMapEvent>) event -> listener.entryAdded((EntryEvent) event);
            };

    /**
     * Converts an {@link uk.org.hecaton.map.listener.EntryRemovedListener} to a {@link uk.org.hecaton.map.impl.ListenerAdapter}.
     */
    private static final ConstructorFunction<MapListener, ListenerAdapter> ENTRY_REMOVED_LISTENER_ADAPTER_CONSTRUCTOR =
            mapListener -> {
                if (!(mapListener instanceof EntryRemovedListener)) {
                    return null;
                }
                final EntryRemovedListener listener = (EntryRemovedListener) mapListener;
                return (ListenerAdapter<IMapEvent>) event -> listener.entryRemoved((EntryEvent) event);
            };


    /**
     * Converts an {@link uk.org.hecaton.map.listener.EntryEvictedListener} to a {@link uk.org.hecaton.map.impl.ListenerAdapter}.
     */
    private static final ConstructorFunction<MapListener, ListenerAdapter> ENTRY_EVICTED_LISTENER_ADAPTER_CONSTRUCTOR =
            mapListener -> {
                if (!(mapListener instanceof EntryEvictedListener)) {
                    return null;
                }
                final EntryEvictedListener listener = (EntryEvictedListener) mapListener;
                return (ListenerAdapter<IMapEvent>) event -> listener.entryEvicted((EntryEvent) event);
            };


    /**
     * Converts an {@link uk.org.hecaton.map.listener.EntryEvictedListener} to a {@link uk.org.hecaton.map.impl.ListenerAdapter}.
     */
    private static final ConstructorFunction<MapListener, ListenerAdapter> ENTRY_EXPIRED_LISTENER_ADAPTER_CONSTRUCTOR =
            mapListener -> {
                if (!(mapListener instanceof EntryExpiredListener)) {
                    return null;
                }
                final EntryExpiredListener listener = (EntryExpiredListener) mapListener;
                return (ListenerAdapter<IMapEvent>) event -> listener.entryExpired((EntryEvent) event);
            };


    /**
     * Converts an {@link uk.org.hecaton.map.listener.EntryUpdatedListener} to a {@link uk.org.hecaton.map.impl.ListenerAdapter}.
     */
    private static final ConstructorFunction<MapListener, ListenerAdapter> ENTRY_UPDATED_LISTENER_ADAPTER_CONSTRUCTOR =
            mapListener -> {
                if (!(mapListener instanceof EntryUpdatedListener)) {
                    return null;
                }
                final EntryUpdatedListener listener = (EntryUpdatedListener) mapListener;
                return (ListenerAdapter<IMapEvent>) event -> listener.entryUpdated((EntryEvent) event);
            };


    /**
     * Converts an {@link uk.org.hecaton.map.listener.MapEvictedListener} to a {@link uk.org.hecaton.map.impl.ListenerAdapter}.
     */
    private static final ConstructorFunction<MapListener, ListenerAdapter> MAP_EVICTED_LISTENER_ADAPTER_CONSTRUCTOR =
            mapListener -> {
                if (!(mapListener instanceof MapEvictedListener)) {
                    return null;
                }
                final MapEvictedListener listener = (MapEvictedListener) mapListener;
                return (ListenerAdapter<IMapEvent>) event -> listener.mapEvicted((MapEvent) event);
            };


    /**
     * Converts an {@link uk.org.hecaton.map.listener.MapClearedListener} to a {@link uk.org.hecaton.map.impl.ListenerAdapter}.
     */
    private static final ConstructorFunction<MapListener, ListenerAdapter> MAP_CLEARED_LISTENER_ADAPTER_CONSTRUCTOR =
            mapListener -> {
                if (!(mapListener instanceof MapClearedListener)) {
                    return null;
                }
                final MapClearedListener listener = (MapClearedListener) mapListener;
                return (ListenerAdapter<IMapEvent>) event -> listener.mapCleared((MapEvent) event);
            };

    /**
     * Converts an {@link uk.org.hecaton.map.listener.EntryMergedListener} to a {@link uk.org.hecaton.map.impl.ListenerAdapter}.
     */
    private static final ConstructorFunction<MapListener, ListenerAdapter> ENTRY_MERGED_LISTENER_ADAPTER_CONSTRUCTOR =
            mapListener -> {
                if (!(mapListener instanceof EntryMergedListener)) {
                    return null;
                }
                final EntryMergedListener listener = (EntryMergedListener) mapListener;
                return (ListenerAdapter<IMapEvent>) event -> listener.entryMerged((EntryEvent) event);
            };


    /**
     * Converts an {@link InvalidationListener} to a {@link uk.org.hecaton.map.impl.ListenerAdapter}.
     */
    private static final ConstructorFunction<MapListener, ListenerAdapter> INVALIDATION_LISTENER =
            mapListener -> {
                if (!(mapListener instanceof InvalidationListener)) {
                    return null;
                }
                final InvalidationListener listener = (InvalidationListener) mapListener;
                return (ListenerAdapter<Invalidation>) listener::onInvalidate;
            };

    /**
     * Converts an {@link EntryLoadedListener} to a {@link uk.org.hecaton.map.impl.ListenerAdapter}.
     */
    private static final ConstructorFunction<MapListener, ListenerAdapter> ENTRY_LOADED_LISTENER_ADAPTER_CONSTRUCTOR =
            mapListener -> {
                if (!(mapListener instanceof EntryLoadedListener)) {
                    return null;
                }
                final EntryLoadedListener listener = (EntryLoadedListener) mapListener;
                return (ListenerAdapter<IMapEvent>) event -> listener.entryLoaded((EntryEvent) event);
            };

    /**
     * Registry for all {@link uk.org.hecaton.map.listener.MapListener} to {@link uk.org.hecaton.map.impl.ListenerAdapter}
     * constructors according to {@link uk.org.hecaton.core.EntryEventType}s.
     */
    private static final Map<EntryEventType, ConstructorFunction<MapListener, ListenerAdapter>> CONSTRUCTORS
            = new EnumMap<>(EntryEventType.class);

    /**
     * Register all {@link uk.org.hecaton.map.impl.ListenerAdapter} constructors
     * according to {@link uk.org.hecaton.core.EntryEventType}s.
     */
    static {
        CONSTRUCTORS.put(EntryEventType.ADDED, ENTRY_ADDED_LISTENER_ADAPTER_CONSTRUCTOR);
        CONSTRUCTORS.put(EntryEventType.LOADED, ENTRY_LOADED_LISTENER_ADAPTER_CONSTRUCTOR);
        CONSTRUCTORS.put(EntryEventType.REMOVED, ENTRY_REMOVED_LISTENER_ADAPTER_CONSTRUCTOR);
        CONSTRUCTORS.put(EntryEventType.EVICTED, ENTRY_EVICTED_LISTENER_ADAPTER_CONSTRUCTOR);
        CONSTRUCTORS.put(EntryEventType.EXPIRED, ENTRY_EXPIRED_LISTENER_ADAPTER_CONSTRUCTOR);
        CONSTRUCTORS.put(EntryEventType.UPDATED, ENTRY_UPDATED_LISTENER_ADAPTER_CONSTRUCTOR);
        CONSTRUCTORS.put(EntryEventType.MERGED, ENTRY_MERGED_LISTENER_ADAPTER_CONSTRUCTOR);
        CONSTRUCTORS.put(EntryEventType.EVICT_ALL, MAP_EVICTED_LISTENER_ADAPTER_CONSTRUCTOR);
        CONSTRUCTORS.put(EntryEventType.CLEAR_ALL, MAP_CLEARED_LISTENER_ADAPTER_CONSTRUCTOR);
        CONSTRUCTORS.put(EntryEventType.INVALIDATION, INVALIDATION_LISTENER);
    }

    private MapListenerAdaptors() {
    }

    /**
     * Creates a {@link uk.org.hecaton.map.impl.ListenerAdapter} array
     * for all event types of {@link uk.org.hecaton.core.EntryEventType}.
     *
     * @param mapListener a {@link uk.org.hecaton.map.listener.MapListener} instance.
     * @return an array of {@link uk.org.hecaton.map.impl.ListenerAdapter}
     */
    public static ListenerAdapter[] createListenerAdapters(MapListener mapListener) {
        EntryEventType[] values = EntryEventType.values();
        ListenerAdapter[] listenerAdapters = new ListenerAdapter[values.length];
        for (EntryEventType eventType : values) {
            listenerAdapters[eventType.ordinal()] = createListenerAdapter(eventType, mapListener);
        }
        return listenerAdapters;
    }

    /**
     * Creates a {@link ListenerAdapter} for a specific {@link uk.org.hecaton.core.EntryEventType}.
     *
     * @param eventType   an {@link uk.org.hecaton.core.EntryEventType}.
     * @param mapListener a {@link uk.org.hecaton.map.listener.MapListener} instance.
     * @return {@link uk.org.hecaton.map.impl.ListenerAdapter} for a specific {@link uk.org.hecaton.core.EntryEventType}
     */
    private static ListenerAdapter createListenerAdapter(EntryEventType eventType, MapListener mapListener) {
        final ConstructorFunction<MapListener, ListenerAdapter> constructorFunction = CONSTRUCTORS.get(eventType);
        if (constructorFunction == null) {
            throw new IllegalArgumentException("First, define a ListenerAdapter for the event EntryEventType." + eventType);
        }
        return constructorFunction.createNew(mapListener);
    }


    /**
     * Wraps a user defined {@link uk.org.hecaton.map.listener.MapListener}
     * into a {@link uk.org.hecaton.map.impl.ListenerAdapter}.
     *
     * @param mapListener a {@link uk.org.hecaton.map.listener.MapListener} instance.
     * @return {@link uk.org.hecaton.map.impl.ListenerAdapter} for the user-defined
     * {@link uk.org.hecaton.map.listener.MapListener}
     */
    static ListenerAdapter createMapListenerAdaptor(MapListener mapListener) {
        return new InternalMapListenerAdapter(mapListener);
    }

    // only used for testing purposes
    public static Map<EntryEventType, ConstructorFunction<MapListener, ListenerAdapter>> getConstructors() {
        return CONSTRUCTORS;
    }
}
