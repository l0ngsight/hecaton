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

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.MessageTaskFactory;
import uk.org.hecaton.client.impl.protocol.MessageTaskFactoryProvider;
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
import uk.org.hecaton.client.impl.protocol.codec.builtin.CustomTypeFactory;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.util.collection.Int2ObjectHashMap;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.vector.SearchResult;
import uk.org.hecaton.vector.impl.DataVectorDocument;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.security.permission.ActionConstants.ACTION_OPTIMIZE;
import static uk.org.hecaton.security.permission.ActionConstants.ACTION_PUT;
import static uk.org.hecaton.security.permission.ActionConstants.ACTION_READ;
import static uk.org.hecaton.security.permission.ActionConstants.ACTION_REMOVE;

/** The client requests of vector collections. */
public class VectorMessageTaskFactoryProvider implements MessageTaskFactoryProvider {

    private final Int2ObjectHashMap<MessageTaskFactory> factories = new Int2ObjectHashMap<>();
    private final Node node;

    public VectorMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
        add(VectorCollectionGetCodec.REQUEST_MESSAGE_TYPE, new VectorMessageTask.Kind<>(
                VectorCollectionGetCodec::decodeRequest, p -> p.name, ACTION_READ, "getAsync",
                (proxy, p) -> proxy.getData(p.key),
                r -> VectorCollectionGetCodec.encodeResponse((DataVectorDocument) r)));
        add(VectorCollectionPutCodec.REQUEST_MESSAGE_TYPE, new VectorMessageTask.Kind<>(
                VectorCollectionPutCodec::decodeRequest, p -> p.name, ACTION_PUT, "putAsync",
                (proxy, p) -> proxy.putData(p.key, p.value),
                r -> VectorCollectionPutCodec.encodeResponse((DataVectorDocument) r)));
        add(VectorCollectionSetCodec.REQUEST_MESSAGE_TYPE, new VectorMessageTask.Kind<>(
                VectorCollectionSetCodec::decodeRequest, p -> p.name, ACTION_PUT, "setAsync",
                (proxy, p) -> proxy.setData(p.key, p.value),
                r -> VectorCollectionSetCodec.encodeResponse()));
        add(VectorCollectionPutIfAbsentCodec.REQUEST_MESSAGE_TYPE, new VectorMessageTask.Kind<>(
                VectorCollectionPutIfAbsentCodec::decodeRequest, p -> p.name, ACTION_PUT, "putIfAbsentAsync",
                (proxy, p) -> proxy.putIfAbsentData(p.key, p.value),
                r -> VectorCollectionPutIfAbsentCodec.encodeResponse((DataVectorDocument) r)));
        add(VectorCollectionPutAllCodec.REQUEST_MESSAGE_TYPE, new VectorMessageTask.Kind<>(
                VectorCollectionPutAllCodec::decodeRequest, p -> p.name, ACTION_PUT, "putAllAsync",
                (proxy, p) -> proxy.putAllData(p.entries),
                r -> VectorCollectionPutAllCodec.encodeResponse()));
        add(VectorCollectionRemoveCodec.REQUEST_MESSAGE_TYPE, new VectorMessageTask.Kind<>(
                VectorCollectionRemoveCodec::decodeRequest, p -> p.name, ACTION_REMOVE, "removeAsync",
                (proxy, p) -> proxy.removeData(p.key),
                r -> VectorCollectionRemoveCodec.encodeResponse((DataVectorDocument) r)));
        add(VectorCollectionDeleteCodec.REQUEST_MESSAGE_TYPE, new VectorMessageTask.Kind<>(
                VectorCollectionDeleteCodec::decodeRequest, p -> p.name, ACTION_REMOVE, "deleteAsync",
                (proxy, p) -> proxy.deleteData(p.key),
                r -> VectorCollectionDeleteCodec.encodeResponse()));
        add(VectorCollectionClearCodec.REQUEST_MESSAGE_TYPE, new VectorMessageTask.Kind<>(
                VectorCollectionClearCodec::decodeRequest, name -> name, ACTION_REMOVE, "clearAsync",
                (proxy, name) -> proxy.clearAsync().toCompletableFuture(),
                r -> VectorCollectionClearCodec.encodeResponse()));
        add(VectorCollectionOptimizeCodec.REQUEST_MESSAGE_TYPE, new VectorMessageTask.Kind<>(
                VectorCollectionOptimizeCodec::decodeRequest, p -> p.name, ACTION_OPTIMIZE, "optimizeAsync",
                (proxy, p) -> proxy.optimizeAsync(p.indexName).toCompletableFuture(),
                r -> VectorCollectionOptimizeCodec.encodeResponse()));
        add(VectorCollectionSizeCodec.REQUEST_MESSAGE_TYPE, new VectorMessageTask.Kind<>(
                VectorCollectionSizeCodec::decodeRequest, name -> name, ACTION_READ, "size",
                (proxy, name) -> CompletableFuture.completedFuture(proxy.size()),
                r -> VectorCollectionSizeCodec.encodeResponse((Long) r)));
        add(VectorCollectionSearchNearVectorCodec.REQUEST_MESSAGE_TYPE, new VectorMessageTask.Kind<>(
                VectorCollectionSearchNearVectorCodec::decodeRequest, p -> p.name, ACTION_READ, "searchAsync",
                (proxy, p) -> proxy.searchData(CustomTypeFactory.toVectorValues(p.vectors), p.options),
                VectorMessageTaskFactoryProvider::encodeSearchResponse));
    }

    @SuppressWarnings("unchecked")
    private static ClientMessage encodeSearchResponse(Object results) {
        return VectorCollectionSearchNearVectorCodec.encodeResponse((List<SearchResult<Data, Data>>) results);
    }

    private <P> void add(int messageType, VectorMessageTask.Kind<P> kind) {
        factories.put(messageType, (clientMessage, connection) -> new VectorMessageTask<>(kind, clientMessage, node,
                connection));
    }

    @Override
    public Int2ObjectHashMap<MessageTaskFactory> getFactories() {
        return factories;
    }
}
