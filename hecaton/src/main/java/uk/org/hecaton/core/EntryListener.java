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

package uk.org.hecaton.core;

import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.MapLoader;
import uk.org.hecaton.map.listener.EntryAddedListener;
import uk.org.hecaton.map.listener.EntryEvictedListener;
import uk.org.hecaton.map.listener.EntryExpiredListener;
import uk.org.hecaton.map.listener.EntryRemovedListener;
import uk.org.hecaton.map.listener.EntryUpdatedListener;
import uk.org.hecaton.map.listener.MapClearedListener;
import uk.org.hecaton.map.listener.MapEvictedListener;

/**
 * Map Entry listener to get notified when a map entry is added,
 * removed, updated, evicted or expired.  Events will fire as a result
 * of operations carried out via the {@link IMap}
 * interface.  Events will not fire, for example, for an entry
 * that comes into the Map via the {@link MapLoader} lifecycle.
 * <p>
 * This interface is here for backward compatibility reasons.
 * For a most appropriate alternative please use/check
 * {@link uk.org.hecaton.map.listener.MapListener} interface.
 *
 * @param <K> the type of key.
 * @param <V> the type of value.
 * @see IMap#addEntryListener
 * @see uk.org.hecaton.map.listener.MapListener
 */
public interface EntryListener<K, V>
        extends EntryAddedListener<K, V>,
        EntryUpdatedListener<K, V>,
        EntryRemovedListener<K, V>,
        EntryEvictedListener<K, V>,
        EntryExpiredListener<K, V>,
        MapClearedListener,
        MapEvictedListener {

}
