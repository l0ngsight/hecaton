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

import uk.org.hecaton.map.IMapEvent;
import uk.org.hecaton.map.EventLostEvent;
import uk.org.hecaton.map.impl.ListenerAdapter;
import uk.org.hecaton.map.impl.MapListenerAdaptors;
import uk.org.hecaton.map.listener.EventLostListener;
import uk.org.hecaton.map.listener.MapListener;
import uk.org.hecaton.internal.util.ConstructorFunction;

/**
 * Responsible for creating {@link uk.org.hecaton.map.QueryCache QueryCache}
 * specific implementations of listeners.
 */
public final class QueryCacheEventListenerAdapters {

    /**
     * Converts an {@link uk.org.hecaton.map.listener.EventLostListener} to a {@link uk.org.hecaton.map.impl.ListenerAdapter}.
     */
    private static final ConstructorFunction<MapListener, ListenerAdapter> EVENT_LOST_LISTENER_ADAPTER =
            mapListener -> {
                if (!(mapListener instanceof EventLostListener)) {
                    return null;
                }
                final EventLostListener listener = (EventLostListener) mapListener;
                return (ListenerAdapter<IMapEvent>) iMapEvent -> listener.eventLost((EventLostEvent) iMapEvent);
            };

    private QueryCacheEventListenerAdapters() {
    }

    static ListenerAdapter[] createQueryCacheListenerAdapters(MapListener mapListener) {
        ListenerAdapter[] mapListenerAdapters = MapListenerAdaptors.createListenerAdapters(mapListener);
        ListenerAdapter eventLostAdapter = EVENT_LOST_LISTENER_ADAPTER.createNew(mapListener);
        ListenerAdapter[] adapters = new ListenerAdapter[mapListenerAdapters.length + 1];
        System.arraycopy(mapListenerAdapters, 0, adapters, 0, mapListenerAdapters.length);
        adapters[mapListenerAdapters.length] = eventLostAdapter;
        return adapters;
    }

    /**
     * Wraps a user defined {@link uk.org.hecaton.map.listener.MapListener}
     * into a {@link uk.org.hecaton.map.impl.ListenerAdapter}.
     *
     * @param mapListener a {@link uk.org.hecaton.map.listener.MapListener} instance.
     * @return {@link uk.org.hecaton.map.impl.ListenerAdapter} for the user-defined
     * {@link uk.org.hecaton.map.listener.MapListener}
     */
    public static ListenerAdapter createQueryCacheListenerAdaptor(MapListener mapListener) {
        return new InternalQueryCacheListenerAdapter(mapListener);
    }
}
