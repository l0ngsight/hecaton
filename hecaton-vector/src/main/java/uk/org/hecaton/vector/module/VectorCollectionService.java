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

package uk.org.hecaton.vector.module;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.vector.VectorCollectionConfig;
import uk.org.hecaton.config.vector.VectorIndexConfig;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.instance.impl.module.VectorCapability;
import uk.org.hecaton.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import uk.org.hecaton.internal.partition.IPartitionService;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.services.ManagedService;
import uk.org.hecaton.internal.services.RemoteService;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.recordstore.MutationObserver;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.vector.SearchOptions;
import uk.org.hecaton.vector.VectorValues;
import uk.org.hecaton.vector.impl.DataSearchResult;
import uk.org.hecaton.vector.impl.DataVectorDocument;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * The member side of vector collections: creates the proxies, keeps the per-partition indexes of
 * the backing maps and searches the partitions the member owns.
 */
public class VectorCollectionService implements ManagedService, RemoteService {

    /** The search hint that sets how many candidates a search explores in each partition. */
    public static final String EF_SEARCH_HINT = "efSearch";

    private final NodeEngine nodeEngine;
    private final ConcurrentMap<String, AtomicReferenceArray<PartitionVectors>> collections = new ConcurrentHashMap<>();

    public VectorCollectionService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @Override
    public void reset() {
        collections.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        collections.clear();
    }

    @Override
    public DistributedObject createDistributedObject(String name, UUID source, boolean local) {
        VectorCollectionConfig config = config(name);
        Config memberConfig = nodeEngine.getConfig();
        Config staticConfig = memberConfig instanceof DynamicConfigurationAwareConfig dynamic
                ? dynamic.getStaticConfig() : memberConfig;
        String mapName = backingMapName(name);
        if (staticConfig.getMapConfigOrNull(mapName) == null) {
            staticConfig.addMapConfig(new MapConfig(mapName)
                    .setBackupCount(config.getBackupCount())
                    .setAsyncBackupCount(config.getAsyncBackupCount())
                    .setInMemoryFormat(InMemoryFormat.OBJECT)
                    .setSplitBrainProtectionName(config.getSplitBrainProtectionName())
                    .setMergePolicyConfig(config.getMergePolicyConfig())
                    .setUserCodeNamespace(config.getUserCodeNamespace()));
        }
        return new VectorCollectionProxy<>(name, nodeEngine, this, config);
    }

    @Override
    public void destroyDistributedObject(String name, boolean local) {
        collections.remove(name);
    }

    static String backingMapName(String name) {
        return VectorCapability.BACKING_MAP_PREFIX + name;
    }

    /** The configuration of a collection; a collection needs one, for its indexes. */
    VectorCollectionConfig config(String name) {
        VectorCollectionConfig config = nodeEngine.getConfig().getVectorCollectionConfigOrNull(name);
        if (config == null || config.getVectorIndexConfigs().isEmpty()) {
            throw new IllegalArgumentException("The vector collection " + name + " has no configuration with indexes;"
                    + " add a vector-collection configuration for it");
        }
        return config;
    }

    MutationObserver<Record> createObserver(RecordStore<Record> recordStore) {
        String name = recordStore.getName().substring(VectorCapability.BACKING_MAP_PREFIX.length());
        VectorCollectionConfig config = nodeEngine.getConfig().getVectorCollectionConfigOrNull(name);
        if (config == null) {
            return null;
        }
        PartitionVectors vectors = new PartitionVectors(config);
        int partitionId = recordStore.getPartitionId();
        AtomicReferenceArray<PartitionVectors> partitions = collections.computeIfAbsent(name,
                n -> new AtomicReferenceArray<>(nodeEngine.getPartitionService().getPartitionCount()));
        partitions.set(partitionId, vectors);
        return new VectorMapObserver(vectors, nodeEngine.getSerializationService(),
                () -> partitions.compareAndSet(partitionId, vectors, null));
    }

    /** Searches the partitions this member owns; the best {@code limit} results, best first. */
    List<DataSearchResult> searchLocal(String name, VectorValues query, SearchOptions options) {
        VectorCollectionConfig config = config(name);
        Map.Entry<String, float[]> target = target(config, query);
        AtomicReferenceArray<PartitionVectors> partitions = collections.get(name);
        if (partitions == null) {
            return List.of();
        }
        int ef = options.getHints().containsKey(EF_SEARCH_HINT)
                ? Integer.parseInt(options.getHints().get(EF_SEARCH_HINT))
                : efSearch(config, target.getKey(), options.getLimit());
        IPartitionService partitionService = nodeEngine.getPartitionService();
        List<DataSearchResult> results = new ArrayList<>();
        for (int partitionId = 0; partitionId < partitions.length(); partitionId++) {
            PartitionVectors vectors = partitions.get(partitionId);
            if (vectors == null || !partitionService.isPartitionOwner(partitionId)) {
                continue;
            }
            for (HnswIndex.Hit<Data, DataVectorDocument> hit
                    : vectors.search(target.getKey(), target.getValue(), options.getLimit(), ef)) {
                results.add(new DataSearchResult(hit.key(), options.isIncludeValue() ? hit.payload().getValue() : null,
                        hit.score(), options.isIncludeVectors() ? hit.payload().getVectors() : null));
            }
        }
        return best(results, options.getLimit());
    }

    void optimizeLocal(String name, String indexName) {
        AtomicReferenceArray<PartitionVectors> partitions = collections.get(name);
        if (partitions == null) {
            return;
        }
        for (int partitionId = 0; partitionId < partitions.length(); partitionId++) {
            PartitionVectors vectors = partitions.get(partitionId);
            if (vectors != null) {
                vectors.optimize(indexName);
            }
        }
    }

    /** The best {@code limit} results, best first, each key once. */
    static List<DataSearchResult> best(List<DataSearchResult> results, int limit) {
        results.sort(Comparator.comparingDouble(DataSearchResult::getScore).reversed());
        List<DataSearchResult> best = new ArrayList<>(Math.min(limit, results.size()));
        Set<Data> keys = new HashSet<>();
        for (DataSearchResult result : results) {
            if (best.size() == limit) {
                break;
            }
            if (keys.add(result.getKey())) {
                best.add(result);
            }
        }
        return best;
    }

    /** The index a query is for and its vector. */
    static Map.Entry<String, float[]> target(VectorCollectionConfig config, VectorValues query) {
        if (query instanceof VectorValues.SingleVectorValues single) {
            if (config.getVectorIndexConfigs().size() != 1) {
                throw new IllegalArgumentException("The vector collection " + config.getName()
                        + " has several indexes; name the index to search");
            }
            return Map.entry(PartitionVectors.indexName(config.getVectorIndexConfigs().get(0)), single.vector());
        }
        if (query instanceof VectorValues.MultiIndexVectorValues multi && multi.indexNameToVector().size() == 1) {
            Map.Entry<String, float[]> entry = multi.indexNameToVector().entrySet().iterator().next();
            for (VectorIndexConfig index : config.getVectorIndexConfigs()) {
                if (PartitionVectors.indexName(index).equals(entry.getKey())) {
                    return entry;
                }
            }
            throw new IllegalArgumentException("The vector collection " + config.getName() + " has no index "
                    + entry.getKey());
        }
        throw new IllegalArgumentException("A search takes one vector, for one index");
    }

    private static int efSearch(VectorCollectionConfig config, String indexName, int limit) {
        for (VectorIndexConfig index : config.getVectorIndexConfigs()) {
            if (PartitionVectors.indexName(index).equals(indexName)) {
                return Math.max(limit, index.getEfConstruction());
            }
        }
        return limit;
    }
}
