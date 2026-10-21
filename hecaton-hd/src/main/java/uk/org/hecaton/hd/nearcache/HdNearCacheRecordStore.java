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

import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.hd.memory.HdMemoryManager;
import uk.org.hecaton.internal.eviction.EvictionChecker;
import uk.org.hecaton.internal.memory.MemoryStats;
import uk.org.hecaton.internal.nearcache.impl.store.BaseHeapNearCacheRecordStore;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.spi.properties.HecatonProperties;

import java.util.concurrent.atomic.LongAdder;

import static uk.org.hecaton.internal.nearcache.NearCacheRecord.TIME_NOT_SET;
import static uk.org.hecaton.internal.util.Clock.currentTimeMillis;
import static uk.org.hecaton.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;
import static uk.org.hecaton.memory.MemoryUnit.MEGABYTES;

/**
 * The record store of a member's {@code NATIVE} Near Cache: the heap Near Cache logic with values
 * in native memory ({@link NativeValueNearCacheRecord}). Besides {@code ENTRY_COUNT}, it supports
 * the native memory max-size policies: {@code USED_NATIVE_MEMORY_SIZE} and {@code _PERCENTAGE}
 * (this Near Cache's values) and {@code FREE_NATIVE_MEMORY_SIZE} and {@code _PERCENTAGE} (the
 * member's free native memory).
 *
 * @param <K> key type
 * @param <V> value type
 */
public class HdNearCacheRecordStore<K, V> extends BaseHeapNearCacheRecordStore<K, V, NativeValueNearCacheRecord> {

    private static final double ONE_HUNDRED = 100D;

    private final HdMemoryManager memory;
    private final MemoryStats memoryStats;
    private final LongAdder footprint;

    public HdNearCacheRecordStore(String name, NearCacheConfig nearCacheConfig, SerializationService serializationService,
                                  ClassLoader classLoader, HecatonProperties properties, HdMemoryManager memory,
                                  MemoryStats memoryStats, LongAdder footprint) {
        super(name, nearCacheConfig, serializationService, classLoader, properties);
        this.memory = memory;
        this.memoryStats = memoryStats;
        this.footprint = footprint;
    }

    @Override
    protected EvictionChecker createNearCacheEvictionChecker(EvictionConfig evictionConfig,
                                                             NearCacheConfig nearCacheConfig) {
        int size = evictionConfig.getSize();
        // called by the super constructor: the checkers read the fields when they run
        return switch (evictionConfig.getMaxSizePolicy()) {
            case USED_NATIVE_MEMORY_SIZE -> () -> footprint.sum() > MEGABYTES.toBytes(size);
            case USED_NATIVE_MEMORY_PERCENTAGE ->
                    () -> footprint.sum() * ONE_HUNDRED / Math.max(1, memoryStats.getMaxNative()) > size;
            case FREE_NATIVE_MEMORY_SIZE -> () -> memoryStats.getFreeNative() < MEGABYTES.toBytes(size);
            case FREE_NATIVE_MEMORY_PERCENTAGE ->
                    () -> memoryStats.getFreeNative() * ONE_HUNDRED / Math.max(1, memoryStats.getMaxNative()) < size;
            default -> super.createNearCacheEvictionChecker(evictionConfig, nearCacheConfig);
        };
    }

    @Override
    protected long getKeyStorageMemoryCost(K key) {
        return key instanceof Data data ? REFERENCE_COST_IN_BYTES + data.getHeapCost() : 0L;
    }

    @Override
    protected long getRecordStorageMemoryCost(NativeValueNearCacheRecord record) {
        return record == null ? 0L : record.valueSize();
    }

    @Override
    protected NativeValueNearCacheRecord createRecord(V value) {
        long creationTime = currentTimeMillis();
        long expirationTime = timeToLiveMillis > 0 ? creationTime + timeToLiveMillis : TIME_NOT_SET;
        return new NativeValueNearCacheRecord(memory, footprint, toData(value), creationTime, expirationTime);
    }

    @Override
    protected void updateRecordValue(NativeValueNearCacheRecord record, V value) {
        record.setValue(toData(value));
    }

    /** Native bytes of the values. */
    public long footprint() {
        return footprint.sum();
    }
}
