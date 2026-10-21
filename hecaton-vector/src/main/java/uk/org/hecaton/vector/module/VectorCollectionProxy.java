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

import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.cluster.memberselector.MemberSelectors;
import uk.org.hecaton.config.vector.VectorCollectionConfig;
import uk.org.hecaton.config.vector.VectorIndexConfig;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spi.impl.AbstractDistributedObject;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.executionservice.ExecutionService;
import uk.org.hecaton.vector.SearchOptions;
import uk.org.hecaton.vector.SearchResults;
import uk.org.hecaton.vector.VectorCollection;
import uk.org.hecaton.vector.VectorDocument;
import uk.org.hecaton.vector.VectorValues;
import uk.org.hecaton.vector.impl.DataSearchResult;
import uk.org.hecaton.vector.impl.DataVectorDocument;
import uk.org.hecaton.vector.impl.SearchResultsImpl;
import uk.org.hecaton.vector.impl.VectorCollectionServiceUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;

/**
 * The member proxy of a vector collection. The documents are in the IMap
 * {@code __vector.<name>} (keys and values serialized, the vectors in the value); searches go to
 * every data member, which searches the partitions it owns.
 *
 * @param <K> the type of the keys
 * @param <V> the type of the values
 */
public class VectorCollectionProxy<K, V> extends AbstractDistributedObject<VectorCollectionService>
        implements VectorCollection<K, V> {

    private final String name;
    private final VectorCollectionConfig config;
    private final IMap<Data, DataVectorDocument> map;

    VectorCollectionProxy(String name, NodeEngine nodeEngine, VectorCollectionService service,
                          VectorCollectionConfig config) {
        super(nodeEngine, service);
        this.name = name;
        this.config = config;
        this.map = nodeEngine.getHecatonInstance().getMap(VectorCollectionService.backingMapName(name));
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return VectorCollectionServiceUtil.SERVICE_NAME;
    }

    @Override
    protected void postDestroy() {
        map.destroy();
    }

    // the API

    @Nonnull
    @Override
    public CompletionStage<VectorDocument<V>> getAsync(@Nonnull K key) {
        return getData(key(key)).thenApply(this::toObject);
    }

    @Nonnull
    @Override
    public CompletionStage<VectorDocument<V>> putAsync(@Nonnull K key, @Nonnull VectorDocument<V> document) {
        return putData(key(key), toData(document)).thenApply(this::toObject);
    }

    @Nonnull
    @Override
    public CompletionStage<Void> putAllAsync(@Nonnull Map<? extends K, VectorDocument<V>> documents) {
        List<Map.Entry<Data, DataVectorDocument>> entries = new ArrayList<>(documents.size());
        documents.forEach((key, document) -> entries.add(Map.entry(key(key), toData(document))));
        return putAllData(entries);
    }

    @Nonnull
    @Override
    public CompletionStage<Void> setAsync(@Nonnull K key, @Nonnull VectorDocument<V> document) {
        return setData(key(key), toData(document));
    }

    @Nonnull
    @Override
    public CompletionStage<VectorDocument<V>> putIfAbsentAsync(@Nonnull K key, @Nonnull VectorDocument<V> document) {
        return putIfAbsentData(key(key), toData(document)).thenApply(this::toObject);
    }

    @Nonnull
    @Override
    public CompletionStage<VectorDocument<V>> removeAsync(@Nonnull K key) {
        return removeData(key(key)).thenApply(this::toObject);
    }

    @Nonnull
    @Override
    public CompletionStage<Void> deleteAsync(@Nonnull K key) {
        return deleteData(key(key));
    }

    @Nonnull
    @Override
    public CompletionStage<Void> optimizeAsync() {
        return optimizeAsync(null);
    }

    @Nonnull
    @Override
    public CompletionStage<Void> optimizeAsync(@Nullable String indexName) {
        List<CompletableFuture<Object>> futures = new ArrayList<>();
        for (Member member : getNodeEngine().getClusterService().getMembers(MemberSelectors.DATA_MEMBER_SELECTOR)) {
            futures.add(getNodeEngine().getOperationService().invokeOnTarget(getServiceName(),
                    new VectorOptimizeOperation(name, indexName), member.getAddress()));
        }
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    @Nonnull
    @Override
    public CompletionStage<Void> clearAsync() {
        return CompletableFuture.runAsync(map::clear, asyncExecutor());
    }

    @Nonnull
    @Override
    public CompletionStage<SearchResults<K, V>> searchAsync(@Nonnull VectorValues vectors, @Nonnull SearchOptions options) {
        return searchData(vectors, options)
                .thenApply(results -> SearchResultsImpl.of(results, getNodeEngine().getSerializationService()));
    }

    @Override
    public long size() {
        return map.size();
    }

    // the same in serialized form, for the client message tasks

    CompletableFuture<DataVectorDocument> getData(Data key) {
        return map.getAsync(key).toCompletableFuture();
    }

    CompletableFuture<DataVectorDocument> putData(Data key, DataVectorDocument document) {
        return map.putAsync(key, validate(document)).toCompletableFuture();
    }

    CompletableFuture<Void> setData(Data key, DataVectorDocument document) {
        return map.setAsync(key, validate(document)).toCompletableFuture();
    }

    CompletableFuture<DataVectorDocument> putIfAbsentData(Data key, DataVectorDocument document) {
        DataVectorDocument valid = validate(document);
        return CompletableFuture.supplyAsync(() -> map.putIfAbsent(key, valid), asyncExecutor());
    }

    CompletableFuture<Void> putAllData(Collection<Map.Entry<Data, DataVectorDocument>> entries) {
        Map<Data, DataVectorDocument> documents = new HashMap<>();
        for (Map.Entry<Data, DataVectorDocument> entry : entries) {
            documents.put(checkNotNull(entry.getKey(), "key"), validate(entry.getValue()));
        }
        return map.putAllAsync(documents).toCompletableFuture();
    }

    CompletableFuture<DataVectorDocument> removeData(Data key) {
        return map.removeAsync(key).toCompletableFuture();
    }

    CompletableFuture<Void> deleteData(Data key) {
        return map.deleteAsync(key).toCompletableFuture().thenApply(removed -> null);
    }

    CompletableFuture<List<DataSearchResult>> searchData(VectorValues vectors, SearchOptions options) {
        checkNotNull(vectors, "vectors");
        checkNotNull(options, "options");
        VectorCollectionService.target(config, vectors);
        CompletableFuture<List<DataSearchResult>> results = CompletableFuture.completedFuture(new ArrayList<>());
        for (Member member : getNodeEngine().getClusterService().getMembers(MemberSelectors.DATA_MEMBER_SELECTOR)) {
            // an invocation future deserializes the response in its own callbacks only
            CompletableFuture<List<DataSearchResult>> memberResults = new CompletableFuture<>();
            getNodeEngine().getOperationService().<VectorSearchOperation.Results>invokeOnTarget(getServiceName(),
                    new VectorSearchOperation(name, vectors, options), member.getAddress()).whenComplete((some, error) -> {
                        if (error == null) {
                            memberResults.complete(some.list());
                        } else {
                            memberResults.completeExceptionally(error);
                        }
                    });
            results = results.thenCombine(memberResults, (all, some) -> {
                all.addAll(some);
                return all;
            });
        }
        return results.thenApply(all -> VectorCollectionService.best(all, options.getLimit()));
    }

    /** Checks that a document has a vector of the right dimension for every index. */
    private DataVectorDocument validate(DataVectorDocument document) {
        checkNotNull(document, "document");
        checkNotNull(document.getValue(), "value");
        checkNotNull(document.getVectors(), "vectors");
        List<VectorIndexConfig> indexes = config.getVectorIndexConfigs();
        if (document.getVectors() instanceof VectorValues.SingleVectorValues && indexes.size() > 1) {
            throw new IllegalArgumentException("The vector collection " + name + " has " + indexes.size()
                    + " indexes; give a vector for each, by index name");
        }
        for (VectorIndexConfig index : indexes) {
            String indexName = PartitionVectors.indexName(index);
            float[] vector = PartitionVectors.vector(document.getVectors(), indexName);
            if (vector == null) {
                throw new IllegalArgumentException("The document has no vector for the index '" + indexName + "'");
            }
            if (vector.length != index.getDimension()) {
                throw new IllegalArgumentException("The vector for the index '" + indexName + "' has " + vector.length
                        + " dimensions; the index has " + index.getDimension());
            }
        }
        return document;
    }

    private Data key(K key) {
        return getNodeEngine().toData(checkNotNull(key, "key"));
    }

    private DataVectorDocument toData(VectorDocument<V> document) {
        return SearchResultsImpl.toData(checkNotNull(document, "document"), serializationService());
    }

    private VectorDocument<V> toObject(DataVectorDocument document) {
        return SearchResultsImpl.toObject(document, serializationService());
    }

    private Executor asyncExecutor() {
        return getNodeEngine().getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR);
    }

    private SerializationService serializationService() {
        return getNodeEngine().getSerializationService();
    }
}
