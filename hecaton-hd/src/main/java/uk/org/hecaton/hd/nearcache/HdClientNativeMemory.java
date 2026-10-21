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

package uk.org.hecaton.hd.nearcache;

import uk.org.hecaton.client.impl.clientside.ClientNativeMemory;
import uk.org.hecaton.config.NativeMemoryConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.hd.memory.HdMemoryManager;
import uk.org.hecaton.hd.memory.HdMemoryStats;
import uk.org.hecaton.internal.memory.MemoryStats;
import uk.org.hecaton.internal.nearcache.NearCacheRecordStore;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.spi.properties.HecatonProperties;

import java.util.concurrent.atomic.LongAdder;

/**
 * The native memory of a client (F11): its {@code NATIVE} Near Caches keep their values in memory
 * allocated as configured by the client's {@link NativeMemoryConfig}, like members' Near Caches.
 */
public final class HdClientNativeMemory implements ClientNativeMemory {

    private final HdMemoryManager memoryManager;
    private final MemoryStats memoryStats;

    HdClientNativeMemory(NativeMemoryConfig config) {
        if (config.getPersistentMemoryConfig().isEnabled()) {
            throw new IllegalStateException("Persistent memory for native memory is not supported by Hecaton yet;"
                    + " disable native-memory.persistent-memory");
        }
        this.memoryManager = HdMemoryManager.create(config);
        this.memoryStats = new HdMemoryStats(memoryManager);
    }

    @Override
    public <K, V> NearCacheRecordStore<K, V> createNearCacheRecordStore(String name, NearCacheConfig nearCacheConfig,
                                                                       SerializationService serializationService,
                                                                       ClassLoader classLoader,
                                                                       HecatonProperties properties) {
        return new HdNearCacheRecordStore<>(name, nearCacheConfig, serializationService, classLoader, properties,
                memoryManager, memoryStats, new LongAdder());
    }

    @Override
    public MemoryStats memoryStats() {
        return memoryStats;
    }

    @Override
    public void dispose() {
        memoryManager.dispose();
    }

    /** Found by clients with {@link java.util.ServiceLoader}. */
    public static final class Factory implements ClientNativeMemory.Factory {

        @Override
        public ClientNativeMemory create(NativeMemoryConfig config) {
            return new HdClientNativeMemory(config);
        }
    }
}
