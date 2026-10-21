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

import uk.org.hecaton.config.vector.VectorCollectionConfig;
import uk.org.hecaton.config.vector.VectorIndexConfig;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.vector.VectorValues;
import uk.org.hecaton.vector.impl.DataVectorDocument;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The indexes of one vector collection in one partition (all of the member's replicas of the
 * partition share them). The partition thread writes them; searches read them from other threads.
 */
final class PartitionVectors {

    /** The name of the index of a collection with one index that has no name. */
    static final String UNNAMED = "";

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<String, HnswIndex<Data, DataVectorDocument>> indexes = new LinkedHashMap<>();

    PartitionVectors(VectorCollectionConfig config) {
        for (VectorIndexConfig index : config.getVectorIndexConfigs()) {
            indexes.put(indexName(index), new HnswIndex<>(index.getMetric(), index.getDimension(), index.getMaxDegree(),
                    index.getEfConstruction()));
        }
    }

    static String indexName(VectorIndexConfig index) {
        return index.getName() == null ? UNNAMED : index.getName();
    }

    void put(Data key, DataVectorDocument document) {
        lock.writeLock().lock();
        try {
            for (Map.Entry<String, HnswIndex<Data, DataVectorDocument>> index : indexes.entrySet()) {
                float[] vector = vector(document.getVectors(), index.getKey());
                if (vector == null) {
                    index.getValue().remove(key);
                } else {
                    index.getValue().put(key, vector, document);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    void remove(Data key) {
        lock.writeLock().lock();
        try {
            for (HnswIndex<Data, DataVectorDocument> index : indexes.values()) {
                index.remove(key);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    void clear() {
        lock.writeLock().lock();
        try {
            indexes.values().forEach(HnswIndex::clear);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Rebuilds an index (all indexes if {@code indexName} is null) without its removed vectors. */
    void optimize(String indexName) {
        lock.writeLock().lock();
        try {
            for (Map.Entry<String, HnswIndex<Data, DataVectorDocument>> index : indexes.entrySet()) {
                if (indexName == null || indexName.equals(index.getKey())) {
                    index.getValue().compact();
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    List<HnswIndex.Hit<Data, DataVectorDocument>> search(String indexName, float[] query, int limit, int ef) {
        lock.readLock().lock();
        try {
            return indexes.get(indexName).search(query, limit, ef);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** The vector of a document for an index: a single vector belongs to the only index. */
    static float[] vector(VectorValues values, String indexName) {
        if (values instanceof VectorValues.SingleVectorValues single) {
            return single.vector();
        }
        if (values instanceof VectorValues.MultiIndexVectorValues multi) {
            return multi.indexNameToVector().get(indexName);
        }
        return null;
    }
}
