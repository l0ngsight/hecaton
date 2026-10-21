/*
 * Copyright (c) 2026, The Hecaton Authors.
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
package uk.org.hecaton.instance.module;

import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapKeyLoader;
import uk.org.hecaton.map.impl.recordstore.RecordStore;

/**
 * Tiered Storage (F12), provided by {@code uk.org.hecaton:hecaton-tiered}: maps with a tiered store keep
 * their values in native memory up to the memory tier's capacity, and the rest on disk.
 */
public non-sealed interface TieredStoreCapability extends MemberCapability {

    /**
     * Creates the record store of one partition of a map with a tiered store.
     */
    RecordStore createMapRecordStore(MapContainer mapContainer, int partitionId, MapKeyLoader keyLoader,
                                     ILogger logger);
}
