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

package uk.org.hecaton.replicatedmap.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.ReplicatedMapConfig;
import uk.org.hecaton.internal.serialization.impl.HeapData;
import uk.org.hecaton.replicatedmap.LocalReplicatedMapStats;
import uk.org.hecaton.internal.monitor.impl.EmptyLocalReplicatedMapStats;
import uk.org.hecaton.internal.monitor.impl.LocalReplicatedMapStatsImpl;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedRecord;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedRecordStore;
import uk.org.hecaton.internal.util.ConstructorFunction;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import static uk.org.hecaton.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static java.lang.Math.max;

/**
 * Provides node local statistics of a replicated map via {@link #getLocalReplicatedMapStats}
 * and also holds all {@link uk.org.hecaton.internal.monitor.impl.LocalReplicatedMapStatsImpl}
 * implementations of all replicated maps.
 */
class LocalReplicatedMapStatsProvider {

    private static final LocalReplicatedMapStats EMPTY_LOCAL_MAP_STATS = new EmptyLocalReplicatedMapStats();

    private final ConcurrentHashMap<String, LocalReplicatedMapStatsImpl> statsMap =
            new ConcurrentHashMap<>();
    private final ConstructorFunction<String, LocalReplicatedMapStatsImpl> statsConstructorFunction =
            arg -> new LocalReplicatedMapStatsImpl();

    private final Config config;
    private final PartitionContainer[] partitionContainers;

    LocalReplicatedMapStatsProvider(Config config, PartitionContainer[] partitionContainers) {
        this.config = config;
        this.partitionContainers = partitionContainers;
    }

    LocalReplicatedMapStatsImpl getLocalReplicatedMapStatsImpl(String name) {
        return getOrPutIfAbsent(statsMap, name, statsConstructorFunction);
    }

    LocalReplicatedMapStats getLocalReplicatedMapStats(String name) {
        ReplicatedMapConfig replicatedMapConfig = getReplicatedMapConfig(name);
        final LocalReplicatedMapStats result;
        if (!replicatedMapConfig.isStatisticsEnabled()) {
            result = EMPTY_LOCAL_MAP_STATS;
        } else {
            LocalReplicatedMapStatsImpl stats = getLocalReplicatedMapStatsImpl(name);
            long hits = 0;
            long count = 0;
            long memoryUsage = 0;
            boolean isBinary = (replicatedMapConfig.getInMemoryFormat() == InMemoryFormat.BINARY);
            for (PartitionContainer container : partitionContainers) {
                ReplicatedRecordStore store = container.getRecordStore(name);
                if (store == null) {
                    continue;
                }
                Iterator<ReplicatedRecord> iterator = store.recordIterator();
                while (iterator.hasNext()) {
                    ReplicatedRecord record = iterator.next();
                    stats.setLastAccessTime(max(stats.getLastAccessTime(), record.getLastAccessTime()));
                    stats.setLastUpdateTime(max(stats.getLastUpdateTime(), record.getUpdateTime()));
                    hits += record.getHits();
                    if (isBinary) {
                        memoryUsage += ((HeapData) record.getValueInternal()).getHeapCost();
                    }
                    count++;
                }
            }
            stats.setOwnedEntryCount(count);
            stats.setHits(hits);
            stats.setOwnedEntryMemoryCost(memoryUsage);
            result = stats;
        }
        return result;
    }

    private ReplicatedMapConfig getReplicatedMapConfig(String name) {
        return config.findReplicatedMapConfig(name);
    }
}
