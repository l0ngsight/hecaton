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

package uk.org.hecaton.client.impl.proxy.txn;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalSetAddCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalSetRemoveCodec;
import uk.org.hecaton.client.impl.protocol.codec.TransactionalSetSizeCodec;
import uk.org.hecaton.client.impl.spi.ClientTransactionContext;
import uk.org.hecaton.collection.impl.set.SetService;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.util.Preconditions;
import uk.org.hecaton.transaction.TransactionalSet;

import static uk.org.hecaton.internal.util.ThreadUtil.getThreadId;

/**
 * Proxy implementation of {@link TransactionalSet}.
 *
 * @param <E> the type of elements in this set
 */
public class ClientTxnSetProxy<E> extends AbstractClientTxnCollectionProxy implements TransactionalSet<E> {

    public ClientTxnSetProxy(String name, ClientTransactionContext transactionContext) {
        super(name, transactionContext);
    }

    @Override
    public boolean add(E e) {
        Preconditions.checkNotNull(e);
        Data value = toData(e);
        ClientMessage request = TransactionalSetAddCodec.encodeRequest(name, getTransactionId(), getThreadId(), value);
        ClientMessage response = invoke(request);
        return TransactionalSetAddCodec.decodeResponse(response);
    }

    @Override
    public boolean remove(E e) {
        Preconditions.checkNotNull(e);
        Data value = toData(e);
        ClientMessage request = TransactionalSetRemoveCodec.encodeRequest(name, getTransactionId(), getThreadId(), value);
        ClientMessage response = invoke(request);
        return TransactionalSetRemoveCodec.decodeResponse(response);
    }

    @Override
    public int size() {
        ClientMessage request = TransactionalSetSizeCodec.encodeRequest(name, getTransactionId(), getThreadId());
        ClientMessage response = invoke(request);
        return TransactionalSetSizeCodec.decodeResponse(response);
    }

    @Override
    public String getServiceName() {
        return SetService.SERVICE_NAME;
    }
}
