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

package uk.org.hecaton.map.impl.eviction;

import uk.org.hecaton.config.EvictionConfig;
import uk.org.hecaton.config.MaxSizePolicy;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.memory.MemoryStats;
import uk.org.hecaton.internal.nearcache.NearCache;
import uk.org.hecaton.internal.util.MemoryInfoAccessor;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.map.impl.MapContainer;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.PartitionContainer;
import uk.org.hecaton.map.impl.nearcache.MapNearCacheManager;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.nearcache.NearCacheStats;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.NodeEngineImpl;

import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;
import static uk.org.hecaton.memory.MemoryUnit.MEGABYTES;
import static java.lang.String.format;

/**
 * Checks whether a specific threshold is exceeded or not according to
 * configured {@link MaxSizePolicy}
 * to start eviction process.
 *
 * @see EvictorImpl#evictionChecker
 */
public class EvictionChecker {

    protected static final double ONE_HUNDRED = 100D;
    private static final int MIN_TRANSLATED_PARTITION_SIZE = 1;

    private final int partitionCount;
    private final ILogger logger;
    private final ClusterService clusterService;
    private final PartitionContainer[] containers;
    private final MemoryInfoAccessor memoryInfoAccessor;
    private final MapNearCacheManager mapNearCacheManager;
    private final AtomicBoolean misconfiguredPerNodeMaxSizeWarningLogged;
    private final NodeEngine nodeEngine;

    public EvictionChecker(MemoryInfoAccessor givenMemoryInfoAccessor, MapServiceContext mapServiceContext) {
        checkNotNull(givenMemoryInfoAccessor, "givenMemoryInfoAccessor cannot be null");
        checkNotNull(mapServiceContext, "mapServiceContext cannot be null");

        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.containers = mapServiceContext.getPartitionContainers();
        this.clusterService = nodeEngine.getClusterService();
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        this.memoryInfoAccessor = givenMemoryInfoAccessor;

        if (logger.isFinestEnabled()) {
            logger.finest("Used memoryInfoAccessor=%s", this.memoryInfoAccessor.getClass().getCanonicalName());
        }

        this.misconfiguredPerNodeMaxSizeWarningLogged = new AtomicBoolean();
    }

    public boolean checkEvictable(RecordStore recordStore) {
        if (recordStore.size() == 0) {
            return false;
        }

        String mapName = recordStore.getName();

        MapContainer mapContainer = recordStore.getMapContainer();
        EvictionConfig evictionConfig = mapContainer.getMapConfig().getEvictionConfig();
        MaxSizePolicy maximumSizePolicy = evictionConfig.getMaxSizePolicy();
        int maxConfiguredSize = evictionConfig.getSize();

        switch (maximumSizePolicy) {
            case PER_NODE:
                return recordStore.size() > toPerPartitionMaxSize(maxConfiguredSize, mapName);
            case PER_PARTITION:
                return recordStore.size() > maxConfiguredSize;
            case USED_HEAP_SIZE:
                return usedHeapInBytes(mapName) > MEGABYTES.toBytes(maxConfiguredSize);
            case FREE_HEAP_SIZE:
                return availableMemoryInBytes() < MEGABYTES.toBytes(maxConfiguredSize);
            case USED_HEAP_PERCENTAGE:
                return (usedHeapInBytes(mapName) * ONE_HUNDRED / Math.max(maxMemoryInBytes(), 1)) > maxConfiguredSize;
            case FREE_HEAP_PERCENTAGE:
                return (availableMemoryInBytes() * ONE_HUNDRED / Math.max(maxMemoryInBytes(), 1)) < maxConfiguredSize;
            default:
                return checkNativeMemoryEvictable(maximumSizePolicy, mapName, maxConfiguredSize);
        }
    }

    /** The native memory policies; a NATIVE record store's owned entry cost is its native footprint. */
    private boolean checkNativeMemoryEvictable(MaxSizePolicy maximumSizePolicy, String mapName, int maxConfiguredSize) {
        switch (maximumSizePolicy) {
            case USED_NATIVE_MEMORY_SIZE:
                return usedHeapInBytes(mapName) > MEGABYTES.toBytes(maxConfiguredSize);
            case USED_NATIVE_MEMORY_PERCENTAGE:
                return (usedHeapInBytes(mapName) * ONE_HUNDRED / Math.max(memoryStats().getMaxNative(), 1))
                        > maxConfiguredSize;
            case FREE_NATIVE_MEMORY_SIZE:
                return memoryStats().getFreeNative() < MEGABYTES.toBytes(maxConfiguredSize);
            case FREE_NATIVE_MEMORY_PERCENTAGE:
                return (memoryStats().getFreeNative() * ONE_HUNDRED / Math.max(memoryStats().getMaxNative(), 1))
                        < maxConfiguredSize;
            default:
                throw new IllegalArgumentException("Not an appropriate max size policy [" + maximumSizePolicy + ']');
        }
    }

    /**
     * Calculates and returns the expected maximum size of an evicted
     * record-store when {@link
     * MaxSizePolicy#PER_NODE
     * PER_NODE} max-size-policy is used.
     */
    private double toPerPartitionMaxSize(int maxConfiguredSize, String mapName) {
        int memberCount = clusterService.getSize(DATA_MEMBER_SELECTOR);
        double translatedPartitionSize = (1D * maxConfiguredSize * memberCount / partitionCount);

        if (translatedPartitionSize < 1) {
            translatedPartitionSize = MIN_TRANSLATED_PARTITION_SIZE;
            logMisconfiguredPerNodeMaxSize(mapName, memberCount);
        }

        return translatedPartitionSize;
    }

    private void logMisconfiguredPerNodeMaxSize(String mapName, int memberCount) {
        if (misconfiguredPerNodeMaxSizeWarningLogged.get()) {
            return;
        }

        if (misconfiguredPerNodeMaxSizeWarningLogged.compareAndSet(false, true)) {
            int minMaxSize = (int) Math.ceil((1D * partitionCount / memberCount));
            int newSize = MIN_TRANSLATED_PARTITION_SIZE * partitionCount / memberCount;
            logger.warning(format("The max size configuration for map \"%s\" does not allow any data in the map. "
                            + "Given the current cluster size of %d members with %d partitions, max size should be at "
                            + "least %d. Map size is forced set to %d for backward compatibility", mapName,
                    memberCount, partitionCount, minMaxSize, newSize));
        }
    }

    private long usedHeapInBytes(String mapName) {
        long usedHeapInBytes = 0L;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            usedHeapInBytes += getRecordStoreHeapCost(mapName, containers[partitionId]);
        }

        NearCache nearCache = mapNearCacheManager.getNearCache(mapName);
        if (nearCache != null) {
            NearCacheStats nearCacheStats = nearCache.getNearCacheStats();
            usedHeapInBytes += nearCacheStats.getOwnedEntryMemoryCost();
        }

        return usedHeapInBytes;
    }

    private long getRecordStoreHeapCost(String mapName, PartitionContainer container) {
        RecordStore existingRecordStore = container.getExistingRecordStore(mapName);
        if (existingRecordStore == null) {
            return 0L;
        }
        return existingRecordStore.getOwnedEntryCost();
    }

    private MemoryStats memoryStats() {
        return ((NodeEngineImpl) nodeEngine).getNode().getNodeExtension().getMemoryStats();
    }

    private long totalMemoryInBytes() {
        return memoryInfoAccessor.getTotalMemory();
    }

    private long freeMemoryInBytes() {
        return memoryInfoAccessor.getFreeMemory();
    }

    private long maxMemoryInBytes() {
        return memoryInfoAccessor.getMaxMemory();
    }

    private long availableMemoryInBytes() {
        return freeMemoryInBytes() + maxMemoryInBytes() - totalMemoryInBytes();
    }
}
