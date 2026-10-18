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
import uk.org.hecaton.internal.nearcache.impl.record.NearCacheDataRecord;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.spi.properties.HecatonProperties;

import static uk.org.hecaton.internal.nearcache.NearCacheRecord.TIME_NOT_SET;
import static uk.org.hecaton.internal.nearcache.impl.record.AbstractNearCacheRecord.NUMBER_OF_BOOLEAN_FIELD_TYPES;
import static uk.org.hecaton.internal.nearcache.impl.record.AbstractNearCacheRecord.NUMBER_OF_INTEGER_FIELD_TYPES;
import static uk.org.hecaton.internal.nearcache.impl.record.AbstractNearCacheRecord.NUMBER_OF_LONG_FIELD_TYPES;
import static uk.org.hecaton.internal.util.Clock.currentTimeMillis;
import static uk.org.hecaton.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;

/**
 * {@link uk.org.hecaton.internal.nearcache.NearCacheRecordStore} implementation for Near Caches
 * with {@link uk.org.hecaton.config.InMemoryFormat#BINARY} in-memory-format.
 *
 * @param <K> the type of the key stored in Near Cache
 * @param <V> the type of the value stored in Near Cache
 */
public class NearCacheDataRecordStore<K, V> extends BaseHeapNearCacheRecordStore<K, V, NearCacheDataRecord> {

    public NearCacheDataRecordStore(String name,
                                    NearCacheConfig nearCacheConfig,
                                    SerializationService serializationService,
                                    ClassLoader classLoader,
                                    HecatonProperties properties) {
        super(name, nearCacheConfig, serializationService, classLoader, properties);
    }

    @Override
    protected long getKeyStorageMemoryCost(K key) {
        if (key instanceof Data data) {
            return
                    // reference to this key data inside map ("store" field)
                    REFERENCE_COST_IN_BYTES
                            // heap cost of this key data
                            + data.getHeapCost();
        } else {
            // memory cost for non-data typed instance is not supported
            return 0L;
        }
    }

    @Override
    protected long getRecordStorageMemoryCost(NearCacheDataRecord record) {
        if (record == null) {
            return 0L;
        }
        // TODO: we don't handle object header (mark, class definition) for heap memory cost
        Data value = record.getValue();
        // reference to this record inside map ("store" field)
        return REFERENCE_COST_IN_BYTES
                // reference to "value" field
                + REFERENCE_COST_IN_BYTES
                // partition Id
                + (Integer.SIZE / Byte.SIZE)
                // "uuid" ref size + 2 long in uuid
                + REFERENCE_COST_IN_BYTES + (2 * (Long.SIZE / Byte.SIZE))
                // heap cost of this value data
                + (value != null ? value.getHeapCost() : 0)
                + NUMBER_OF_LONG_FIELD_TYPES * (Long.SIZE / Byte.SIZE)
                + NUMBER_OF_INTEGER_FIELD_TYPES * (Integer.SIZE / Byte.SIZE)
                + NUMBER_OF_BOOLEAN_FIELD_TYPES;
    }

    @Override
    protected NearCacheDataRecord createRecord(V value) {
        Data dataValue = toData(value);
        long creationTime = currentTimeMillis();
        if (timeToLiveMillis > 0) {
            return new NearCacheDataRecord(dataValue, creationTime, creationTime + timeToLiveMillis);
        } else {
            return new NearCacheDataRecord(dataValue, creationTime, TIME_NOT_SET);
        }
    }

    @Override
    protected void updateRecordValue(NearCacheDataRecord record, V value) {
        record.setValue(toData(value));
    }
}
