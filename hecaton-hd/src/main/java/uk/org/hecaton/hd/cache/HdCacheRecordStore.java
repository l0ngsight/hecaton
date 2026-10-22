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
package uk.org.hecaton.hd.cache;

import uk.org.hecaton.cache.impl.AbstractCacheRecordStore;
import uk.org.hecaton.cache.impl.AbstractCacheService;
import uk.org.hecaton.cache.impl.CacheEntryProcessorEntry;
import uk.org.hecaton.cache.impl.record.CacheRecord;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.hd.HdModule;
import uk.org.hecaton.instance.impl.module.NativeMemoryCapability;
import uk.org.hecaton.internal.eviction.EvictionChecker;
import uk.org.hecaton.internal.memory.MemoryStats;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.NodeEngineImpl;

import static uk.org.hecaton.internal.namespace.NamespaceUtil.callWithNamespace;
import static uk.org.hecaton.memory.MemoryUnit.MEGABYTES;

/**
 * The record store of a partition of a {@code NATIVE} cache: the cache record store logic over a
 * {@link NativeCacheRecordMap}. Supports the native memory max-size policies
 * ({@code USED_NATIVE_MEMORY_SIZE} and {@code _PERCENTAGE}: the cache's native memory on this
 * member; {@code FREE_NATIVE_MEMORY_SIZE} and {@code _PERCENTAGE}: the member's free native memory)
 * and {@code ENTRY_COUNT}.
 */
public class HdCacheRecordStore extends AbstractCacheRecordStore<CacheRecord, NativeCacheRecordMap> {

    private static final double ONE_HUNDRED = 100D;

    public HdCacheRecordStore(String cacheNameWithPrefix, int partitionId, NodeEngine nodeEngine,
                              AbstractCacheService cacheService) {
        super(cacheNameWithPrefix, partitionId, nodeEngine, cacheService);
    }

    // called by the super constructor: only its fields are set
    private HdModule module() {
        return (HdModule) ((NodeEngineImpl) nodeEngine).getNode().getMemberModules()
                .require(NativeMemoryCapability.class, "NATIVE in-memory format");
    }

    @Override
    protected NativeCacheRecordMap createRecordCacheMap() {
        HdModule module = module();
        return new NativeCacheRecordMap(module.memoryManager(), nodeEngine.getSerializationService(), cacheContext,
                module.cacheFootprint(name));
    }

    @Override
    protected EvictionChecker createCacheEvictionChecker(int size, MaxSizePolicy maxSizePolicy) {
        if (maxSizePolicy == null) {
            throw new IllegalArgumentException("Max-Size policy cannot be null");
        }
        HdModule module = module();
        MemoryStats stats = module.memoryStats();
        long capacity = Math.max(1, stats.getMaxNative());
        return switch (maxSizePolicy) {
            case USED_NATIVE_MEMORY_SIZE -> () -> module.cacheFootprint(name).sum() > MEGABYTES.toBytes(size);
            case USED_NATIVE_MEMORY_PERCENTAGE -> () -> module.cacheFootprint(name).sum() * ONE_HUNDRED / capacity > size;
            case FREE_NATIVE_MEMORY_SIZE -> () -> stats.getFreeNative() < MEGABYTES.toBytes(size);
            case FREE_NATIVE_MEMORY_PERCENTAGE -> () -> stats.getFreeNative() * ONE_HUNDRED / capacity < size;
            case ENTRY_COUNT -> super.createCacheEvictionChecker(size, maxSizePolicy);
            default -> throw new IllegalArgumentException("Max-size policy " + maxSizePolicy
                    + " cannot be used with a NATIVE cache");
        };
    }

    @Override
    protected CacheEntryProcessorEntry createCacheEntryProcessorEntry(Data key, CacheRecord record, long now,
                                                                      int completionId) {
        return new CacheEntryProcessorEntry(key, record, this, now, completionId);
    }

    @Override
    protected CacheRecord createRecord(Object value, long creationTime, long expiryTime) {
        evictIfRequired();
        markExpirable(expiryTime);
        return new NativeCacheRecord(valueToData(value), creationTime, expiryTime);
    }

    @Override
    protected Data valueToData(Object value) {
        return callWithNamespace(nodeEngine, cacheConfig.getUserCodeNamespace(), () -> cacheService.toData(value));
    }

    @Override
    protected Object dataToValue(Data data) {
        return callWithNamespace(nodeEngine, cacheConfig.getUserCodeNamespace(), () -> ss.toObject(data));
    }

    @Override
    protected Object recordToValue(CacheRecord record) {
        return record.getValue();
    }

    @Override
    protected Data recordToData(CacheRecord record) {
        Object value = record.getValue();
        if (value == null) {
            return null;
        }
        return value instanceof Data data ? data : valueToData(value);
    }

    @Override
    protected Data toHeapData(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Data data) {
            return data;
        }
        if (obj instanceof CacheRecord<?, ?> record) {
            return toHeapData(record.getValue());
        }
        return valueToData(obj);
    }

    @Override
    public void disposeDeferredBlocks() {
        // records never hand out native memory
    }

    @Override
    public void close(boolean onShutdown) {
        super.close(onShutdown);
        records.destroy();
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        records.destroy();
    }
}
