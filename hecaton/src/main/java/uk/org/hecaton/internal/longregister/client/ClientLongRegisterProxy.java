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

package uk.org.hecaton.internal.longregister.client;

import uk.org.hecaton.client.impl.ClientDelegatingFuture;
import uk.org.hecaton.client.impl.clientside.ClientMessageDecoder;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.spi.ClientContext;
import uk.org.hecaton.client.impl.spi.ClientProxy;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocationFuture;
import uk.org.hecaton.core.IFunction;
import uk.org.hecaton.cp.IAtomicLong;
import uk.org.hecaton.internal.longregister.client.codec.LongRegisterAddAndGetCodec;
import uk.org.hecaton.internal.longregister.client.codec.LongRegisterDecrementAndGetCodec;
import uk.org.hecaton.internal.longregister.client.codec.LongRegisterGetAndAddCodec;
import uk.org.hecaton.internal.longregister.client.codec.LongRegisterGetAndIncrementCodec;
import uk.org.hecaton.internal.longregister.client.codec.LongRegisterGetAndSetCodec;
import uk.org.hecaton.internal.longregister.client.codec.LongRegisterGetCodec;
import uk.org.hecaton.internal.longregister.client.codec.LongRegisterIncrementAndGetCodec;
import uk.org.hecaton.internal.longregister.client.codec.LongRegisterSetCodec;
import uk.org.hecaton.partition.strategy.StringPartitioningStrategy;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;

import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;

/**
 * Proxy implementation of {@link IAtomicLong}.
 */
@SuppressWarnings("checkstyle:methodcount")
public class ClientLongRegisterProxy extends ClientProxy implements IAtomicLong {

    private int partitionId;

    public ClientLongRegisterProxy(String serviceName, String objectId, ClientContext context) {
        super(serviceName, objectId, context);
    }

    @Override
    protected void onInitialize() {
        String partitionKey = StringPartitioningStrategy.getPartitionKey(name);
        partitionId = getContext().getPartitionService().getPartitionId(partitionKey);
    }

    @Override
    public <R> R apply(IFunction<Long, R> function) {
        return applyAsync(function).join();
    }

    @Override
    public void alter(IFunction<Long, Long> function) {
        alterAsync(function).join();
    }

    @Override
    public long alterAndGet(IFunction<Long, Long> function) {
        return alterAndGetAsync(function).join();
    }

    @Override
    public long getAndAlter(IFunction<Long, Long> function) {
        return getAndAlterAsync(function).join();
    }

    @Override
    public long addAndGet(long delta) {
        return addAndGetAsync(delta).join();
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long decrementAndGet() {
        return decrementAndGetAsync().join();
    }

    @Override
    public long getAndDecrement() {
        return getAndDecrementAsync().join();
    }

    @Override
    public long get() {
        return getAsync().join();
    }

    @Override
    public long getAndAdd(long delta) {
        return getAndAddAsync(delta).join();
    }

    @Override
    public long getAndSet(long newValue) {
        return getAndSetAsync(newValue).join();
    }

    @Override
    public long incrementAndGet() {
        return incrementAndGetAsync().join();
    }

    @Override
    public long getAndIncrement() {
        return getAndIncrementAsync().join();
    }

    @Override
    public void set(long newValue) {
        setAsync(newValue).join();
    }

    @Override
    public InternalCompletableFuture<Long> addAndGetAsync(long delta) {
        ClientMessage request = LongRegisterAddAndGetCodec.encodeRequest(name, delta);
        return invokeOnPartitionAsync(request, LongRegisterAddAndGetCodec::decodeResponse);
    }

    @Override
    public InternalCompletableFuture<Boolean> compareAndSetAsync(long expect, long update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalCompletableFuture<Long> decrementAndGetAsync() {
        ClientMessage request = LongRegisterDecrementAndGetCodec.encodeRequest(name);
        return invokeOnPartitionAsync(request, LongRegisterDecrementAndGetCodec::decodeResponse);
    }

    @Override
    public InternalCompletableFuture<Long> getAndDecrementAsync() {
        return getAndAddAsync(-1);
    }

    @Override
    public InternalCompletableFuture<Long> getAsync() {
        ClientMessage request = LongRegisterGetCodec.encodeRequest(name);
        return invokeOnPartitionAsync(request, LongRegisterGetCodec::decodeResponse);
    }

    @Override
    public InternalCompletableFuture<Long> getAndAddAsync(long delta) {
        ClientMessage request = LongRegisterGetAndAddCodec.encodeRequest(name, delta);
        return invokeOnPartitionAsync(request, LongRegisterGetAndAddCodec::decodeResponse);
    }

    @Override
    public InternalCompletableFuture<Long> getAndSetAsync(long newValue) {
        ClientMessage request = LongRegisterGetAndSetCodec.encodeRequest(name, newValue);
        return invokeOnPartitionAsync(request, LongRegisterGetAndSetCodec::decodeResponse);
    }

    @Override
    public InternalCompletableFuture<Long> incrementAndGetAsync() {
        ClientMessage request = LongRegisterIncrementAndGetCodec.encodeRequest(name);
        return invokeOnPartitionAsync(request, LongRegisterIncrementAndGetCodec::decodeResponse);
    }

    @Override
    public InternalCompletableFuture<Long> getAndIncrementAsync() {
        ClientMessage request = LongRegisterGetAndIncrementCodec.encodeRequest(name);
        return invokeOnPartitionAsync(request, LongRegisterGetAndIncrementCodec::decodeResponse);
    }

    @Override
    public InternalCompletableFuture<Void> setAsync(long newValue) {
        ClientMessage request = LongRegisterSetCodec.encodeRequest(name, newValue);
        return invokeOnPartitionAsync(request, clientMessage -> null);
    }

    @Override
    public InternalCompletableFuture<Void> alterAsync(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalCompletableFuture<Long> alterAndGetAsync(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InternalCompletableFuture<Long> getAndAlterAsync(IFunction<Long, Long> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <R> InternalCompletableFuture<R> applyAsync(IFunction<Long, R> function) {
        throw new UnsupportedOperationException();
    }

    private <T> ClientDelegatingFuture<T> invokeOnPartitionAsync(ClientMessage clientMessage,
            ClientMessageDecoder clientMessageDecoder) {
        try {
            ClientInvocationFuture future = new ClientInvocation(getClient(), clientMessage, getName(), partitionId).invoke();
            return new ClientDelegatingFuture<>(future, getSerializationService(), clientMessageDecoder);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public String toString() {
        return "IAtomicLong{" + "name='" + name + '\'' + '}';
    }
}
