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

package uk.org.hecaton.client.impl.clientside;

import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.internal.nearcache.NearCache;
import uk.org.hecaton.internal.nearcache.NearCacheRecordStore;
import uk.org.hecaton.internal.nearcache.impl.DefaultNearCache;
import uk.org.hecaton.internal.nearcache.impl.DefaultNearCacheManager;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.impl.executionservice.TaskScheduler;
import uk.org.hecaton.spi.properties.HecatonProperties;

import javax.annotation.Nullable;

/**
 * The Near Cache manager of a client: {@code NATIVE} Near Caches keep their values in the client's
 * native memory when it has one, and are on-heap {@code BINARY} Near Caches otherwise.
 */
class ClientNearCacheManager extends DefaultNearCacheManager {

    private final ClientNativeMemory nativeMemory;
    private final ILogger logger;

    ClientNearCacheManager(SerializationService ss, TaskScheduler scheduler, ClassLoader classLoader,
                           HecatonProperties properties, @Nullable ClientNativeMemory nativeMemory, ILogger logger) {
        super(ss, scheduler, classLoader, properties);
        this.nativeMemory = nativeMemory;
        this.logger = logger;
    }

    @Override
    protected <K, V> NearCache<K, V> createNearCache(String name, NearCacheConfig nearCacheConfig) {
        if (nearCacheConfig.getInMemoryFormat() != InMemoryFormat.NATIVE) {
            return super.createNearCache(name, nearCacheConfig);
        }
        if (nativeMemory == null) {
            logger.warning("The Near Cache of " + name + " is NATIVE, but the client has no native memory (it needs"
                    + " native-memory enabled and uk.org.hecaton:hecaton-hd on a Java 25+ client): it keeps its"
                    + " entries on the heap, in BINARY format");
            return super.createNearCache(name, onHeap(nearCacheConfig));
        }
        NearCacheRecordStore<K, V> recordStore = nativeMemory.createNearCacheRecordStore(name, nearCacheConfig,
                serializationService, classLoader, properties);
        return new DefaultNearCache<>(name, nearCacheConfig, recordStore, serializationService, scheduler, classLoader,
                properties);
    }

    /**
     * The configuration as a {@code BINARY} Near Cache: native size policies become an entry count,
     * and keys stay serialized, as the proxies of {@code NATIVE} Near Caches pass them.
     */
    private static NearCacheConfig onHeap(NearCacheConfig nearCacheConfig) {
        NearCacheConfig copy = new NearCacheConfig(nearCacheConfig).setInMemoryFormat(InMemoryFormat.BINARY)
                .setSerializeKeys(true);
        EvictionConfig eviction = new EvictionConfig(copy.getEvictionConfig());
        if (eviction.getMaxSizePolicy() != MaxSizePolicy.ENTRY_COUNT) {
            eviction.setMaxSizePolicy(MaxSizePolicy.ENTRY_COUNT).setSize(EvictionConfig.DEFAULT_MAX_ENTRY_COUNT);
        }
        return copy.setEvictionConfig(eviction);
    }
}
