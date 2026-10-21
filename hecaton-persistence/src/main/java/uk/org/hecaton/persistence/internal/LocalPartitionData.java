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

import uk.org.hecaton.persistence.internal.cluster.LocalData;
import uk.org.hecaton.persistence.internal.map.MapPartitionLoader;
import uk.org.hecaton.persistence.internal.store.StructureRegistry.Structure;
import uk.org.hecaton.spi.impl.NodeEngine;

import java.util.Set;
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
        Set<Integer> partitions = partitionsOnDisk;
        if (partitions == null) {
            partitions = store.partitionsOnDisk();
            partitionsOnDisk = partitions;
        }
        return partitions.contains(partitionId);
    }

    @Override
    public void load(int partitionId, boolean owner) {
        MapPartitionLoader mapLoader = new MapPartitionLoader(nodeEngine.get(), partitionId);
        store.setLoading(partitionId, true);
        try {
            store.log(partitionId).replay((structureId, key, value) -> {
                Structure structure = store.structure(structureId);
                if (structure != null && structure.kind() == Structure.MAP) {
                    mapLoader.load(structure.name(), key, value);
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
    }
}
