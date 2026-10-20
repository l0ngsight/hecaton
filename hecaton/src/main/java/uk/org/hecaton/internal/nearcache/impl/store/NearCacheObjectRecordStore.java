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

package uk.org.hecaton.internal.nearcache.impl.store;

import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.internal.nearcache.impl.record.NearCacheObjectRecord;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.spi.properties.HecatonProperties;

import static uk.org.hecaton.internal.nearcache.NearCacheRecord.TIME_NOT_SET;
import static uk.org.hecaton.internal.util.Clock.currentTimeMillis;

/**
 * {@link uk.org.hecaton.internal.nearcache.NearCacheRecordStore} implementation for Near Caches
 * with {@link uk.org.hecaton.config.InMemoryFormat#OBJECT} in-memory-format.
 *
 * @param <K> the type of the key stored in Near Cache
 * @param <V> the type of the value stored in Near Cache
 */
public class NearCacheObjectRecordStore<K, V> extends BaseHeapNearCacheRecordStore<K, V, NearCacheObjectRecord<V>> {

    public NearCacheObjectRecordStore(String name,
                                      NearCacheConfig nearCacheConfig,
                                      SerializationService serializationService,
                                      ClassLoader classLoader,
                                      HecatonProperties properties) {
        super(name, nearCacheConfig, serializationService, classLoader, properties);
    }

    @Override
    protected long getKeyStorageMemoryCost(K key) {
        // memory cost for "OBJECT" in memory format is totally not supported, so just return zero
        return 0L;
    }

    @Override
    protected long getRecordStorageMemoryCost(NearCacheObjectRecord record) {
        // memory cost for "OBJECT" in memory format is totally not supported, so just return zero
        return 0L;
    }

    @Override
    protected NearCacheObjectRecord<V> createRecord(V value) {
        value = toValue(value);
        long creationTime = currentTimeMillis();
        if (timeToLiveMillis > 0) {
            return new NearCacheObjectRecord<>(value, creationTime, creationTime + timeToLiveMillis);
        } else {
            return new NearCacheObjectRecord<>(value, creationTime, TIME_NOT_SET);
        }
    }

    @Override
    protected void updateRecordValue(NearCacheObjectRecord<V> record, V value) {
        record.setValue(toValue(value));
    }
}
