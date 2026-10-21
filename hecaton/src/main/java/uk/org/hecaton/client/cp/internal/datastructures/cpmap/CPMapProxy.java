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
package uk.org.hecaton.client.cp.internal.datastructures.cpmap;

import uk.org.hecaton.client.impl.ClientDelegatingFuture;
import uk.org.hecaton.client.impl.clientside.ClientMessageDecoder;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.CPGroupDestroyCPObjectCodec;
import uk.org.hecaton.client.impl.protocol.codec.CPMapCompareAndSetCodec;
import uk.org.hecaton.client.impl.protocol.codec.CPMapDeleteCodec;
import uk.org.hecaton.client.impl.protocol.codec.CPMapGetCodec;
import uk.org.hecaton.client.impl.protocol.codec.CPMapPutCodec;
import uk.org.hecaton.client.impl.protocol.codec.CPMapPutIfAbsentCodec;
import uk.org.hecaton.client.impl.protocol.codec.CPMapRemoveCodec;
import uk.org.hecaton.client.impl.protocol.codec.CPMapSetCodec;
import uk.org.hecaton.client.impl.spi.ClientContext;
import uk.org.hecaton.client.impl.spi.ClientProxy;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocationFuture;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.CPMap;
import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.cp.internal.datastructures.cpmap.CPMapService;
import uk.org.hecaton.internal.serialization.Data;

import javax.annotation.Nonnull;

import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;

/**
 * Client-side proxy of {@link CPMap}
 *
 * @param <K> key type
 * @param <V> value type
 */
public class CPMapProxy<K, V> extends ClientProxy implements CPMap<K, V> {

    private final RaftGroupId groupId;
    private final String objectName;

    public CPMapProxy(ClientContext context, RaftGroupId groupId, String proxyName, String objectName) {
        super(CPMapService.SERVICE_NAME, proxyName, context);
        this.groupId = groupId;
        this.objectName = objectName;
    }

    @Override
    public V put(@Nonnull K key, @Nonnull V value) {
        Data keyData = keyData(key);
        Data valueData = valueData(value);
        return invoke(CPMapPutCodec.encodeRequest(groupId, objectName, keyData, valueData), CPMapPutCodec::decodeResponse);
    }

    @Override
    public V putIfAbsent(@Nonnull K key, @Nonnull V value) {
        Data keyData = keyData(key);
        Data valueData = valueData(value);
        return invoke(CPMapPutIfAbsentCodec.encodeRequest(groupId, objectName, keyData, valueData),
                CPMapPutIfAbsentCodec::decodeResponse);
    }

    @Override
    public void set(@Nonnull K key, @Nonnull V value) {
        Data keyData = keyData(key);
        Data valueData = valueData(value);
        invoke(CPMapSetCodec.encodeRequest(groupId, objectName, keyData, valueData), CPMapSetCodec::decodeResponse);
    }

    @Override
    public V remove(@Nonnull K key) {
        return invoke(CPMapRemoveCodec.encodeRequest(groupId, objectName, keyData(key)), CPMapRemoveCodec::decodeResponse);
    }

    @Override
    public void delete(@Nonnull K key) {
        invoke(CPMapDeleteCodec.encodeRequest(groupId, objectName, keyData(key)), CPMapDeleteCodec::decodeResponse);
    }

    @Override
    public boolean compareAndSet(@Nonnull K key, @Nonnull V expectedValue, @Nonnull V newValue) {
        Data keyData = keyData(key);
        checkNotNull(expectedValue, "Expected value cannot be null");
        checkNotNull(newValue, "New value cannot be null");
        ClientMessage request = CPMapCompareAndSetCodec.encodeRequest(groupId, objectName, keyData,
                toData(expectedValue), toData(newValue));
        return invoke(request, CPMapCompareAndSetCodec::decodeResponse);
    }

    @Override
    public V get(@Nonnull K key) {
        return invoke(CPMapGetCodec.encodeRequest(groupId, objectName, keyData(key)), CPMapGetCodec::decodeResponse);
    }

    private <R> R invoke(ClientMessage request, ClientMessageDecoder decoder) {
        ClientInvocationFuture future = new ClientInvocation(getClient(), request, name).invoke();
        return new ClientDelegatingFuture<R>(future, getSerializationService(), decoder).joinInternal();
    }

    private Data keyData(K key) {
        checkNotNull(key, "Key cannot be null");
        return toData(key);
    }

    private Data valueData(V value) {
        checkNotNull(value, "Value cannot be null");
        return toData(value);
    }

    @Override
    public void onDestroy() {
        ClientMessage request = CPGroupDestroyCPObjectCodec.encodeRequest(groupId, getServiceName(), objectName);
        new ClientInvocation(getClient(), request, name).invoke().joinInternal();
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    public CPGroupId getGroupId() {
        return groupId;
    }
}
