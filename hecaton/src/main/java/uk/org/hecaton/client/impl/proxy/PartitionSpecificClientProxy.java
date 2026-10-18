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
import uk.org.hecaton.client.impl.spi.ClientContext;
import uk.org.hecaton.client.impl.spi.ClientProxy;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocation;
import uk.org.hecaton.client.impl.spi.impl.ClientInvocationFuture;
import uk.org.hecaton.internal.util.ExceptionUtil;
import uk.org.hecaton.partition.strategy.StringPartitioningStrategy;

import java.util.concurrent.Future;

import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;

/**
 * Base class for proxies of distributed objects that lives in on partition.
 */
abstract class PartitionSpecificClientProxy extends ClientProxy {

    private int partitionId;

    protected PartitionSpecificClientProxy(String serviceName, String objectName, ClientContext context) {
        super(serviceName, objectName, context);
    }

    @Override
    protected void onInitialize() {
        String partitionKey = StringPartitioningStrategy.getPartitionKey(name);
        partitionId = getContext().getPartitionService().getPartitionId(partitionKey);
    }

    protected ClientMessage invokeOnPartition(ClientMessage req) {
        return invokeOnPartition(req, partitionId);
    }

    protected <T> T invokeOnPartitionInterruptibly(ClientMessage clientMessage) throws InterruptedException {
        return invokeOnPartitionInterruptibly(clientMessage, partitionId);
    }

    protected <T> ClientDelegatingFuture<T> invokeOnPartitionAsync(ClientMessage clientMessage,
                                                                   ClientMessageDecoder clientMessageDecoder) {
        try {
            ClientInvocationFuture future = new ClientInvocation(getClient(), clientMessage, getName(), partitionId).invoke();
            return new ClientDelegatingFuture<>(future, getSerializationService(), clientMessageDecoder);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected <T> T invokeOnPartition(ClientMessage clientMessage, long invocationTimeoutSeconds) {
        try {
            ClientInvocation clientInvocation = new ClientInvocation(getClient(), clientMessage, getName(), partitionId);
            clientInvocation.setInvocationTimeoutMillis(invocationTimeoutSeconds);
            final Future future = clientInvocation.invoke();
            return (T) future.get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected <T> T invokeOnPartitionInterruptibly(ClientMessage clientMessage,
                                                 long invocationTimeoutSeconds) throws InterruptedException {
        try {
            ClientInvocation clientInvocation = new ClientInvocation(getClient(), clientMessage, getName(), partitionId);
            clientInvocation.setInvocationTimeoutMillis(invocationTimeoutSeconds);
            final Future future = clientInvocation.invoke();
            return (T) future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrowAllowInterrupted(e);
        }
    }
}
