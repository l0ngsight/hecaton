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
package uk.org.hecaton.persistence.internal;

import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.internal.partition.PartitionReplicaVersionManager;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.persistence.internal.cache.CachePartitionLoader;
import uk.org.hecaton.persistence.internal.cluster.LocalData;
import uk.org.hecaton.persistence.internal.map.MapPartitionLoader;
import uk.org.hecaton.persistence.internal.store.StructureRegistry.Structure;
import uk.org.hecaton.spi.impl.NodeEngine;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/** {@link LocalData} on the member's {@link DataStore}. */
final class LocalPartitionData implements LocalData {

    private final DataStore store;
    private final Supplier<NodeEngine> nodeEngine;
    private volatile Set<Integer> partitionsOnDisk;

    LocalPartitionData(DataStore store, Supplier<NodeEngine> nodeEngine) {
        this.store = store;
        this.nodeEngine = nodeEngine;
    }

    @Override
    public boolean hasPartition(int partitionId) {
        return partitionsOnDisk().contains(partitionId);
    }

    private Set<Integer> partitionsOnDisk() {
        Set<Integer> partitions = partitionsOnDisk;
        if (partitions == null) {
            partitions = ConcurrentHashMap.newKeySet();
            partitions.addAll(store.partitionsOnDisk());
            partitionsOnDisk = partitions;
        }
        return partitions;
    }

    @Override
    public void load(int partitionId, boolean owner) {
        MapPartitionLoader mapLoader = new MapPartitionLoader(nodeEngine.get(), partitionId);
        CachePartitionLoader cacheLoader = new CachePartitionLoader(nodeEngine.get(), partitionId);
        store.setLoading(partitionId, true);
        try {
            store.log(partitionId).replay((structureId, key, value) -> {
                Structure structure = store.structure(structureId);
                if (structure == null) {
                    return;
                }
                if (structure.kind() == Structure.MAP) {
                    mapLoader.load(structure.name(), key, value);
                } else if (structure.kind() == Structure.CACHE) {
                    cacheLoader.load(structure.name(), store.config(structureId), key, value);
                }
            });
            if (owner) {
                mapLoader.populateIndexes();
            }
        } finally {
            store.setLoading(partitionId, false);
        }
    }

    @Override
    public void delete(int partitionId) {
        store.deletePartition(partitionId);
        partitionsOnDisk().remove(partitionId);
    }

    @Override
    public void markBackupsStale(int partitionId) {
        NodeEngine engine = nodeEngine.get();
        MapService mapService = engine.getService(MapService.SERVICE_NAME);
        PartitionReplicaVersionManager versions =
                ((InternalPartitionService) engine.getPartitionService()).getPartitionReplicaVersionManager();
        for (String mapName : mapService.getMapServiceContext().getPartitionContainer(partitionId).getMaps().keySet()) {
            versions.incrementPartitionReplicaVersions(partitionId, MapService.getObjectNamespace(mapName),
                    InternalPartition.MAX_BACKUP_COUNT);
        }
    }

    @Override
    public void deleteAll() {
        for (Integer partitionId : List.copyOf(partitionsOnDisk())) {
            delete(partitionId);
        }
    }
}
