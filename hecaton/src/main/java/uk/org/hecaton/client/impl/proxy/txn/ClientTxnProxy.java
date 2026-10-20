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

import uk.org.hecaton.client.impl.connection.ClientConnection;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.client.impl.protocol.codec.ClientDestroyProxyCodec;
import uk.org.hecaton.client.impl.spi.ClientTransactionContext;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.partition.strategy.StringPartitioningStrategy;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.transaction.TransactionalObject;

import java.util.UUID;

abstract class ClientTxnProxy implements TransactionalObject {

    final String name;
    final ClientTransactionContext transactionContext;

    ClientTxnProxy(String name, ClientTransactionContext transactionContext) {
        this.name = name;
        this.transactionContext = transactionContext;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getPartitionKey() {
        return StringPartitioningStrategy.getPartitionKey(name);
    }

    @Override
    public final void destroy() {
        onDestroy();
        ClientMessage request = ClientDestroyProxyCodec.encodeRequest(name, getServiceName());
        invoke(request);
    }

    abstract void onDestroy();

    final ClientMessage invoke(ClientMessage request) {
        HecatonClientInstanceImpl client = transactionContext.getClient();
        ClientConnection connection = transactionContext.getConnection();
        return ClientTransactionUtil.invoke(request, getName(), client, connection);
    }

    UUID getTransactionId() {
        return transactionContext.getTxnId();
    }

    Data toData(Object obj) {
        return transactionContext.getClient().getSerializationService().toData(obj);
    }

    Object toObject(Data data) {
        return transactionContext.getClient().getSerializationService().toObject(data);
    }

    SerializationService getSerializationService() {
        return transactionContext.getClient().getSerializationService();
    }
}
