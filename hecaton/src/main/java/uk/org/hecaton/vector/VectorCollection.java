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

package uk.org.hecaton.vector;

import uk.org.hecaton.config.vector.VectorCollectionConfig;
import uk.org.hecaton.core.DistributedObject;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.annotation.Beta;
import uk.org.hecaton.vector.impl.VectorCollectionServiceUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * A partitioned collection of documents (a value and one or more vectors) with vector similarity
 * search (F20). Members need {@code uk.org.hecaton:hecaton-vector}. The collection's indexes are
 * configured with {@link VectorCollectionConfig}: one unnamed index, or several named ones.
 *
 * @param <K> key type
 * @param <V> value type
 */
@Beta
public interface VectorCollection<K, V> extends DistributedObject {

    /** The document of the key, or {@code null}. */
    @Nonnull
    CompletionStage<VectorDocument<V>> getAsync(@Nonnull K key);

    /** Stores the document, returning the previous one, or {@code null}. */
    @Nonnull
    CompletionStage<VectorDocument<V>> putAsync(@Nonnull K key, @Nonnull VectorDocument<V> document);

    /** Stores the documents. */
    @Nonnull
    CompletionStage<Void> putAllAsync(@Nonnull Map<? extends K, VectorDocument<V>> documents);

    /** Stores the document. */
    @Nonnull
    CompletionStage<Void> setAsync(@Nonnull K key, @Nonnull VectorDocument<V> document);

    /** Stores the document if the key has none, returning the existing one, or {@code null}. */
    @Nonnull
    CompletionStage<VectorDocument<V>> putIfAbsentAsync(@Nonnull K key, @Nonnull VectorDocument<V> document);

    /** Removes the document of the key, returning it, or {@code null}. */
    @Nonnull
    CompletionStage<VectorDocument<V>> removeAsync(@Nonnull K key);

    /** Removes the document of the key. */
    @Nonnull
    CompletionStage<Void> deleteAsync(@Nonnull K key);

    /** Rebuilds the indexes, which may make searches more accurate after many updates. */
    @Nonnull
    CompletionStage<Void> optimizeAsync();

    /** Rebuilds an index; {@code null} for the unnamed index. */
    @Nonnull
    CompletionStage<Void> optimizeAsync(@Nullable String indexName);

    /** Removes all documents. */
    @Nonnull
    CompletionStage<Void> clearAsync();

    /**
     * The documents most similar to the vectors (one vector, for an index of the collection),
     * best first, at most {@link SearchOptions#getLimit()} of them.
     */
    @Nonnull
    CompletionStage<SearchResults<K, V>> searchAsync(@Nonnull VectorValues vectors, @Nonnull SearchOptions options);

    /** The number of documents. */
    long size();

    /** The vector collection with the given configuration, which is added to the cluster's configuration. */
    @Nonnull
    static <K, V> VectorCollection<K, V> getCollection(@Nonnull HecatonInstance instance,
                                                       @Nonnull VectorCollectionConfig config) {
        instance.getConfig().addVectorCollectionConfig(config);
        return getCollection(instance, config.getName());
    }

    /** The vector collection of the given name; its configuration must exist. */
    @Nonnull
    static <K, V> VectorCollection<K, V> getCollection(@Nonnull HecatonInstance instance, @Nonnull String name) {
        return instance.getDistributedObject(VectorCollectionServiceUtil.SERVICE_NAME, name);
    }
}
