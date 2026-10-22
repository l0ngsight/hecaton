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
package uk.org.hecaton.instance.impl.module;

import uk.org.hecaton.cache.impl.AbstractCacheService;
import uk.org.hecaton.cache.impl.ICacheRecordStore;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.internal.memory.MemoryStats;
import uk.org.hecaton.internal.nearcache.NearCacheRecordStore;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapKeyLoader;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.properties.HecatonProperties;

/**
 * High-Density memory (F11), provided by {@code uk.org.hecaton:hecaton-hd}: data of structures
 * with the {@code NATIVE} in-memory format is kept outside the Java heap.
 */
public non-sealed interface NativeMemoryCapability extends MemberCapability {

    /**
     * Memory statistics of the member, including its native memory.
     */
    MemoryStats memoryStats();

    /**
     * Creates the record store of one partition of a map with the {@code NATIVE} in-memory format.
     */
    RecordStore createMapRecordStore(MapContainer mapContainer, int partitionId, MapKeyLoader keyLoader,
                                     ILogger logger);

    /**
     * Creates the record store of one partition of a cache with the {@code NATIVE} in-memory format.
     */
    ICacheRecordStore createCacheRecordStore(String cacheNameWithPrefix, int partitionId, NodeEngine nodeEngine,
                                             AbstractCacheService cacheService);

    /**
     * Creates the record store of a member's Near Cache with the {@code NATIVE} in-memory format.
     */
    <K, V> NearCacheRecordStore<K, V> createNearCacheRecordStore(String name, NearCacheConfig nearCacheConfig,
                                                                 SerializationService serializationService,
                                                                 ClassLoader classLoader, HecatonProperties properties);
}
