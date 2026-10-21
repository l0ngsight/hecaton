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
package uk.org.hecaton.hd;

import uk.org.hecaton.cache.impl.AbstractCacheService;
import uk.org.hecaton.cache.impl.ICacheRecordStore;
import uk.org.hecaton.config.NativeMemoryConfig;
import uk.org.hecaton.hd.cache.HdCacheRecordStore;
import uk.org.hecaton.hd.map.HdRecordStore;
import uk.org.hecaton.hd.memory.HdMemoryManager;
import uk.org.hecaton.hd.memory.HdMemoryStats;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.instance.module.MemberModule;
import uk.org.hecaton.instance.module.NativeMemoryCapability;
import uk.org.hecaton.internal.memory.DefaultMemoryStats;
import uk.org.hecaton.internal.memory.MemoryStats;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapKeyLoader;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.spi.impl.NodeEngine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * The High-Density memory module ({@code hecaton-hd}): keeps the data of {@code NATIVE}
 * structures in native memory, allocated as configured by {@link NativeMemoryConfig}.
 */
public class HdModule implements MemberModule, NativeMemoryCapability {

    private final Map<String, LongAdder> cacheFootprints = new ConcurrentHashMap<>();
    private volatile HdMemoryManager memoryManager;
    private volatile MemoryStats memoryStats = new DefaultMemoryStats();

    @Override
    public String name() {
        return "hd";
    }

    @Override
    public void init(Node node) {
        NativeMemoryConfig config = node.getConfig().getNativeMemoryConfig();
        if (!config.isEnabled()) {
            return;
        }
        if (config.getPersistentMemoryConfig().isEnabled()) {
            throw new IllegalStateException("Persistent memory for native memory is not supported by Hecaton yet;"
                    + " disable native-memory.persistent-memory");
        }
        memoryManager = HdMemoryManager.create(config);
        memoryStats = new HdMemoryStats(memoryManager);
        node.getLogger(HdModule.class).info("Native memory: " + config.getAllocatorType() + " allocator, capacity "
                + config.getCapacity().bytes() + " bytes");
    }

    /**
     * The member's native memory manager.
     *
     * @throws IllegalStateException if native memory is not enabled
     */
    public HdMemoryManager memoryManager() {
        HdMemoryManager manager = memoryManager;
        if (manager == null) {
            throw new IllegalStateException("Native memory is not enabled (native-memory.enabled)");
        }
        return manager;
    }

    /** Counts the native bytes of the records of a cache on this member, over all its partitions. */
    public LongAdder cacheFootprint(String cacheNameWithPrefix) {
        return cacheFootprints.computeIfAbsent(cacheNameWithPrefix, name -> new LongAdder());
    }

    @Override
    public ICacheRecordStore createCacheRecordStore(String cacheNameWithPrefix, int partitionId, NodeEngine nodeEngine,
                                                    AbstractCacheService cacheService) {
        return new HdCacheRecordStore(cacheNameWithPrefix, partitionId, nodeEngine, cacheService);
    }

    @Override
    public MemoryStats memoryStats() {
        return memoryStats;
    }

    @Override
    public RecordStore createMapRecordStore(MapContainer mapContainer, int partitionId, MapKeyLoader keyLoader,
                                            ILogger logger) {
        return new HdRecordStore(mapContainer, partitionId, keyLoader, logger, memoryManager());
    }

    @Override
    public void shutdown(boolean terminate) {
        HdMemoryManager manager = memoryManager;
        if (manager != null) {
            manager.dispose();
        }
    }
}
