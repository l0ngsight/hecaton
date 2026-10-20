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

package uk.org.hecaton.client.impl.proxy;

import uk.org.hecaton.client.impl.ClientDelegatingFuture;
import uk.org.hecaton.client.impl.clientside.ClientMessageDecoder;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.RingbufferAddAllCodec;
import uk.org.hecaton.client.impl.protocol.codec.RingbufferAddCodec;
import uk.org.hecaton.client.impl.protocol.codec.RingbufferCapacityCodec;
import uk.org.hecaton.client.impl.protocol.codec.RingbufferHeadSequenceCodec;
import uk.org.hecaton.client.impl.protocol.codec.RingbufferReadManyCodec;
import uk.org.hecaton.client.impl.protocol.codec.RingbufferReadOneCodec;
import uk.org.hecaton.client.impl.protocol.codec.RingbufferRemainingCapacityCodec;
import uk.org.hecaton.client.impl.protocol.codec.RingbufferSizeCodec;
import uk.org.hecaton.client.impl.protocol.codec.RingbufferTailSequenceCodec;
import uk.org.hecaton.client.impl.spi.ClientContext;
import uk.org.hecaton.client.impl.spi.ClientProxy;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocationFuture;
import uk.org.hecaton.core.IFunction;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.partition.strategy.StringPartitioningStrategy;
import uk.org.hecaton.ringbuffer.OverflowPolicy;
import uk.org.hecaton.ringbuffer.ReadResultSet;
import uk.org.hecaton.ringbuffer.Ringbuffer;
import uk.org.hecaton.ringbuffer.StaleSequenceException;
import uk.org.hecaton.ringbuffer.impl.ReadResultSetImpl;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;

import javax.annotation.Nonnull;
import java.util.Collection;

import static uk.org.hecaton.internal.util.CollectionUtil.objectToDataCollection;
import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static uk.org.hecaton.internal.util.Preconditions.checkFalse;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNegative;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;
import static uk.org.hecaton.internal.util.Preconditions.checkTrue;
import static uk.org.hecaton.ringbuffer.impl.RingbufferProxy.MAX_BATCH_SIZE;
import static uk.org.hecaton.spi.impl.InternalCompletableFuture.completedExceptionally;
import static java.lang.String.format;

/**
 * Proxy implementation of {@link Ringbuffer}.
 *
 * @param <E> the type of elements in this ringbuffer
 */
public class ClientRingbufferProxy<E> extends ClientProxy implements Ringbuffer<E> {

    private ClientMessageDecoder readManyAsyncResponseDecoder;

    private int partitionId;
    private volatile long capacity = -1;

    public ClientRingbufferProxy(String serviceName, String objectName, ClientContext context) {
        super(serviceName, objectName, context);
    }

    @Override
    protected void onInitialize() {
        String partitionKey = StringPartitioningStrategy.getPartitionKey(name);
        partitionId = getContext().getPartitionService().getPartitionId(partitionKey);

        readManyAsyncResponseDecoder = clientMessage -> {
            final RingbufferReadManyCodec.ResponseParameters params = RingbufferReadManyCodec.decodeResponse(clientMessage);
            final ReadResultSetImpl readResultSet = new ReadResultSetImpl(
                    params.readCount, params.items, params.itemSeqs, params.nextSeq);
            readResultSet.setSerializationService(getSerializationService());
            return readResultSet;
        };
    }

    @Override
    public long capacity() {
        if (capacity == -1) {
            ClientMessage request = RingbufferCapacityCodec.encodeRequest(name);
            ClientMessage response = invoke(request, partitionId);
            capacity = RingbufferCapacityCodec.decodeResponse(response);
        }
        return capacity;
    }

    @Override
    public long size() {
        ClientMessage request = RingbufferSizeCodec.encodeRequest(name);
        ClientMessage response = invoke(request, partitionId);
        return RingbufferSizeCodec.decodeResponse(response);
    }

    @Override
    public long tailSequence() {
        ClientMessage request = RingbufferTailSequenceCodec.encodeRequest(name);
        ClientMessage response = invoke(request, partitionId);
        return RingbufferTailSequenceCodec.decodeResponse(response);
    }

    @Override
    public long headSequence() {
        ClientMessage request = RingbufferHeadSequenceCodec.encodeRequest(name);
        ClientMessage response = invoke(request, partitionId);
        return RingbufferHeadSequenceCodec.decodeResponse(response);
    }

    @Override
    public long remainingCapacity() {
        ClientMessage request = RingbufferRemainingCapacityCodec.encodeRequest(name);
        ClientMessage response = invoke(request, partitionId);
        return RingbufferRemainingCapacityCodec.decodeResponse(response);
    }

    @Override
    public long add(@Nonnull E item) {
        checkNotNull(item, "item can't be null");

        Data element = toData(item);
        ClientMessage request = RingbufferAddCodec.encodeRequest(name, OverflowPolicy.OVERWRITE.getId(), element);
        ClientMessage response = invoke(request, partitionId);
        return RingbufferAddCodec.decodeResponse(response);
    }

    @Override
    public InternalCompletableFuture<Long> addAsync(@Nonnull E item, @Nonnull OverflowPolicy overflowPolicy) {
        checkNotNull(item, "item can't be null");
        checkNotNull(overflowPolicy, "overflowPolicy can't be null");

        Data element = toData(item);
        ClientMessage request = RingbufferAddCodec.encodeRequest(name, overflowPolicy.getId(), element);
        try {
            ClientInvocationFuture future = new ClientInvocation(getClient(), request, getName(), partitionId).invoke();
            return new ClientDelegatingFuture<>(future, getSerializationService(), RingbufferAddCodec::decodeResponse);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public E readOne(long sequence) throws InterruptedException {
        checkSequence(sequence);

        ClientMessage request = RingbufferReadOneCodec.encodeRequest(name, sequence);
        ClientMessage response = invoke(request, partitionId);
        return toObject(RingbufferReadOneCodec.decodeResponse(response));
    }

    @Override
    public InternalCompletableFuture<Long> addAllAsync(@Nonnull Collection<? extends E> collection,
                                                       @Nonnull OverflowPolicy overflowPolicy) {
        checkNotNull(collection, "collection can't be null");
        checkNotNull(overflowPolicy, "overflowPolicy can't be null");
        checkFalse(collection.isEmpty(), "collection can't be empty");
        checkTrue(collection.size() <= MAX_BATCH_SIZE, "collection can't be larger than " + MAX_BATCH_SIZE);

        Collection<Data> dataCollection = objectToDataCollection(collection, getSerializationService());
        ClientMessage request = RingbufferAddAllCodec.encodeRequest(name, dataCollection, overflowPolicy.getId());

        try {
            ClientInvocationFuture future = new ClientInvocation(getClient(), request, getName(), partitionId).invoke();
            return new ClientDelegatingFuture<>(future, getSerializationService(), RingbufferAddAllCodec::decodeResponse);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public InternalCompletableFuture<ReadResultSet<E>> readManyAsync(long startSequence, int minCount,
                                                                     int maxCount, IFunction<E, Boolean> filter) {
        checkSequence(startSequence);
        checkNotNegative(minCount, "minCount can't be smaller than 0");
        checkTrue(maxCount >= minCount, "maxCount should be equal or larger than minCount");

        try {
            capacity();
        } catch (Throwable e) {
            return completedExceptionally(e);
        }

        checkTrue(maxCount <= capacity, "the maxCount should be smaller than or equal to the capacity");
        checkTrue(maxCount <= MAX_BATCH_SIZE, "maxCount can't be larger than " + MAX_BATCH_SIZE);

        ClientMessage request = RingbufferReadManyCodec.encodeRequest(
                name,
                startSequence,
                minCount,
                maxCount,
                toData(filter));

        try {
            ClientInvocationFuture invocationFuture = new ClientInvocation(getClient(), request, getName(), partitionId).invoke();
            return new ClientDelegatingFuture<>(invocationFuture, getSerializationService(),
                    readManyAsyncResponseDecoder);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private static void checkSequence(long sequence) {
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence can't be smaller than 0, but was: " + sequence);
        }
    }

    protected <T> T invoke(ClientMessage clientMessage, int partitionId) {
        try {
            ClientInvocationFuture future = new ClientInvocation(getClient(), clientMessage, getName(), partitionId).invoke();
            return (T) future.joinInternal();
        } catch (StaleSequenceException e) {
            long l = headSequence();
            throw new StaleSequenceException(e.getMessage(), l);
        }
    }

    @Override
    public String toString() {
        return format("Ringbuffer{name='%s'}", name);
    }
}
