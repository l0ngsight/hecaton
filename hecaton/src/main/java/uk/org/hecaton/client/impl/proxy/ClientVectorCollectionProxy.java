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

package uk.org.hecaton.client.impl.proxy;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.VectorCollectionClearCodec;
import uk.org.hecaton.client.impl.protocol.codec.VectorCollectionDeleteCodec;
import uk.org.hecaton.client.impl.protocol.codec.VectorCollectionGetCodec;
import uk.org.hecaton.client.impl.protocol.codec.VectorCollectionOptimizeCodec;
import uk.org.hecaton.client.impl.protocol.codec.VectorCollectionPutAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.VectorCollectionPutCodec;
import uk.org.hecaton.client.impl.protocol.codec.VectorCollectionPutIfAbsentCodec;
import uk.org.hecaton.client.impl.protocol.codec.VectorCollectionRemoveCodec;
import uk.org.hecaton.client.impl.protocol.codec.VectorCollectionSearchNearVectorCodec;
import uk.org.hecaton.client.impl.protocol.codec.VectorCollectionSetCodec;
import uk.org.hecaton.client.impl.protocol.codec.VectorCollectionSizeCodec;
import uk.org.hecaton.client.impl.spi.ClientContext;
import uk.org.hecaton.client.impl.spi.ClientProxy;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.vector.SearchOptions;
import uk.org.hecaton.vector.SearchResults;
import uk.org.hecaton.vector.VectorCollection;
import uk.org.hecaton.vector.VectorDocument;
import uk.org.hecaton.vector.VectorValues;
import uk.org.hecaton.vector.impl.DataVectorDocument;
import uk.org.hecaton.vector.impl.SearchResultsImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;

/**
 * Client proxy of a vector collection (F20).
 */
public class ClientVectorCollectionProxy<K, V> extends ClientProxy implements VectorCollection<K, V> {

    public ClientVectorCollectionProxy(String serviceName, String name, ClientContext context) {
        super(serviceName, name, context);
    }

    @Nonnull
    @Override
    public CompletionStage<VectorDocument<V>> getAsync(@Nonnull K key) {
        return onKey(key, VectorCollectionGetCodec.encodeRequest(name, toData(checkNotNull(key, "key"))))
                .thenApply(message -> document(VectorCollectionGetCodec.decodeResponse(message)));
    }

    @Nonnull
    @Override
    public CompletionStage<VectorDocument<V>> putAsync(@Nonnull K key, @Nonnull VectorDocument<V> document) {
        return onKey(key, VectorCollectionPutCodec.encodeRequest(name, toData(checkNotNull(key, "key")), data(document)))
                .thenApply(message -> document(VectorCollectionPutCodec.decodeResponse(message)));
    }

    @Nonnull
    @Override
    public CompletionStage<Void> putAllAsync(@Nonnull Map<? extends K, VectorDocument<V>> documents) {
        List<Map.Entry<Data, DataVectorDocument>> entries = new ArrayList<>(documents.size());
        documents.forEach((key, document) -> entries.add(new SimpleImmutableEntry<>(toData(key), data(document))));
        return anywhere(VectorCollectionPutAllCodec.encodeRequest(name, entries)).thenApply(message -> null);
    }

    @Nonnull
    @Override
    public CompletionStage<Void> setAsync(@Nonnull K key, @Nonnull VectorDocument<V> document) {
        return onKey(key, VectorCollectionSetCodec.encodeRequest(name, toData(checkNotNull(key, "key")), data(document)))
                .thenApply(message -> null);
    }

    @Nonnull
    @Override
    public CompletionStage<VectorDocument<V>> putIfAbsentAsync(@Nonnull K key, @Nonnull VectorDocument<V> document) {
        return onKey(key, VectorCollectionPutIfAbsentCodec.encodeRequest(name, toData(checkNotNull(key, "key")),
                data(document)))
                .thenApply(message -> document(VectorCollectionPutIfAbsentCodec.decodeResponse(message)));
    }

    @Nonnull
    @Override
    public CompletionStage<VectorDocument<V>> removeAsync(@Nonnull K key) {
        return onKey(key, VectorCollectionRemoveCodec.encodeRequest(name, toData(checkNotNull(key, "key"))))
                .thenApply(message -> document(VectorCollectionRemoveCodec.decodeResponse(message)));
    }

    @Nonnull
    @Override
    public CompletionStage<Void> deleteAsync(@Nonnull K key) {
        return onKey(key, VectorCollectionDeleteCodec.encodeRequest(name, toData(checkNotNull(key, "key"))))
                .thenApply(message -> null);
    }

    @Nonnull
    @Override
    public CompletionStage<Void> optimizeAsync() {
        return optimizeAsync(null);
    }

    @Nonnull
    @Override
    public CompletionStage<Void> optimizeAsync(@Nullable String indexName) {
        return anywhere(VectorCollectionOptimizeCodec.encodeRequest(name, indexName, UuidUtil.newUnsecureUUID()))
                .thenApply(message -> null);
    }

    @Nonnull
    @Override
    public CompletionStage<Void> clearAsync() {
        return anywhere(VectorCollectionClearCodec.encodeRequest(name)).thenApply(message -> null);
    }

    @Nonnull
    @Override
    public CompletionStage<SearchResults<K, V>> searchAsync(@Nonnull VectorValues vectors, @Nonnull SearchOptions options) {
        return anywhere(VectorCollectionSearchNearVectorCodec.encodeRequest(name, checkNotNull(vectors, "vectors"),
                checkNotNull(options, "options")))
                .thenApply(message -> SearchResultsImpl.of(VectorCollectionSearchNearVectorCodec.decodeResponse(message),
                        getSerializationService()));
    }

    @Override
    public long size() {
        return VectorCollectionSizeCodec.decodeResponse(invoke(VectorCollectionSizeCodec.encodeRequest(name)));
    }

    private CompletableFuture<ClientMessage> onKey(K key, ClientMessage request) {
        int partitionId = getContext().getPartitionService().getPartitionId(toData(key));
        return new ClientInvocation(getClient(), request, getName(), partitionId).invoke();
    }

    private CompletableFuture<ClientMessage> anywhere(ClientMessage request) {
        return new ClientInvocation(getClient(), request, getName()).invoke();
    }

    private DataVectorDocument data(VectorDocument<V> document) {
        return SearchResultsImpl.toData(checkNotNull(document, "document"), getSerializationService());
    }

    private VectorDocument<V> document(DataVectorDocument document) {
        return SearchResultsImpl.toObject(document, getSerializationService());
    }
}
