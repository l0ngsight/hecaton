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

import uk.org.hecaton.config.NativeMemoryConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.internal.memory.MemoryStats;
import uk.org.hecaton.internal.nearcache.NearCacheRecordStore;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.spi.properties.HecatonProperties;

/**
 * The native memory of a client, for its {@code NATIVE} Near Caches (F11). The client finds a
 * {@link Factory} with {@link java.util.ServiceLoader}: {@code uk.org.hecaton:hecaton-hd}
 * provides one, on Java 25+.
 */
public interface ClientNativeMemory {

    /** Creates the record store of a {@code NATIVE} Near Cache, whose values live in this memory. */
    <K, V> NearCacheRecordStore<K, V> createNearCacheRecordStore(String name, NearCacheConfig nearCacheConfig,
                                                                SerializationService serializationService,
                                                                ClassLoader classLoader, HecatonProperties properties);

    MemoryStats memoryStats();

    /** Releases the memory, once the client's Near Caches are destroyed. */
    void dispose();

    /** Creates the native memory of a client. */
    interface Factory {

        ClientNativeMemory create(NativeMemoryConfig config);
    }
}
