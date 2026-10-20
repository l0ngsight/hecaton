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
import uk.org.hecaton.client.impl.spi.ClientTransactionContext;
import uk.org.hecaton.client.impl.spi.impl.ClientTransactionManagerServiceImpl;
import uk.org.hecaton.collection.impl.list.ListService;
import uk.org.hecaton.collection.impl.queue.QueueService;
import uk.org.hecaton.collection.impl.set.SetService;
import uk.org.hecaton.transaction.TransactionalList;
import uk.org.hecaton.transaction.TransactionalMap;
import uk.org.hecaton.transaction.TransactionalMultiMap;
import uk.org.hecaton.transaction.TransactionalQueue;
import uk.org.hecaton.transaction.TransactionalSet;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.multimap.impl.MultiMapService;
import uk.org.hecaton.transaction.TransactionException;
import uk.org.hecaton.transaction.TransactionNotActiveException;
import uk.org.hecaton.transaction.TransactionOptions;
import uk.org.hecaton.transaction.TransactionalObject;
import uk.org.hecaton.transaction.impl.Transaction;
import uk.org.hecaton.transaction.impl.TransactionalObjectKey;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Provides a context to perform transactional operations: beginning/committing transactions, but also retrieving
 * transactional data-structures like the {@link TransactionalMap}.
 *
 * Provides client instance and client connection proxies that need to be accessed for sending invocations.
 */
public class TransactionContextProxy implements ClientTransactionContext {

    final ClientTransactionManagerServiceImpl transactionManager;
    final HecatonClientInstanceImpl client;
    final TransactionProxy transaction;
    final ClientConnection connection;

    private final Map<TransactionalObjectKey, TransactionalObject> txnObjectMap =
            new HashMap<>(2);

    public TransactionContextProxy(@Nonnull ClientTransactionManagerServiceImpl transactionManager,
                                   @Nonnull TransactionOptions options) {
        this.transactionManager = transactionManager;
        this.client = transactionManager.getClient();
        try {
            connection = transactionManager.connect();
        } catch (Exception e) {
            throw new TransactionException("Could not obtain a connection!", e);
        }
        this.transaction = new TransactionProxy(client, options, connection);
    }

    @Override
    public UUID getTxnId() {
        return transaction.getTxnId();
    }

    @Override
    public void beginTransaction() {
        transaction.begin();
    }

    @Override
    public void commitTransaction() throws TransactionException {
        transaction.commit();
    }

    @Override
    public void rollbackTransaction() {
        transaction.rollback();
    }

    @Override
    public <K, V> TransactionalMap<K, V> getMap(String name) {
        return getTransactionalObject(MapService.SERVICE_NAME, name);
    }

    @Override
    public <E> TransactionalQueue<E> getQueue(String name) {
        return getTransactionalObject(QueueService.SERVICE_NAME, name);
    }

    @Override
    public <K, V> TransactionalMultiMap<K, V> getMultiMap(String name) {
        return getTransactionalObject(MultiMapService.SERVICE_NAME, name);
    }

    @Override
    public <E> TransactionalList<E> getList(String name) {
        return getTransactionalObject(ListService.SERVICE_NAME, name);
    }

    @Override
    public <E> TransactionalSet<E> getSet(String name) {
        return getTransactionalObject(SetService.SERVICE_NAME, name);
    }

    @Override
    public <T extends TransactionalObject> T getTransactionalObject(String serviceName, String name) {
        if (transaction.getState() != Transaction.State.ACTIVE) {
            throw new TransactionNotActiveException("No transaction is found while accessing "
                    + "transactional object -> " + serviceName + "[" + name + "]!");
        }
        TransactionalObjectKey key = new TransactionalObjectKey(serviceName, name);
        TransactionalObject obj = txnObjectMap.get(key);
        if (obj == null) {
            if (serviceName.equals(QueueService.SERVICE_NAME)) {
                obj = new ClientTxnQueueProxy(name, this);
            } else if (serviceName.equals(MapService.SERVICE_NAME)) {
                obj = new ClientTxnMapProxy(name, this);
            } else if (serviceName.equals(MultiMapService.SERVICE_NAME)) {
                obj = new ClientTxnMultiMapProxy(name, this);
            } else if (serviceName.equals(ListService.SERVICE_NAME)) {
                obj = new ClientTxnListProxy(name, this);
            } else if (serviceName.equals(SetService.SERVICE_NAME)) {
                obj = new ClientTxnSetProxy(name, this);
            }

            if (obj == null) {
                throw new IllegalArgumentException("Service[" + serviceName + "] is not transactional!");
            }
            txnObjectMap.put(key, obj);
        }
        return (T) obj;
    }

    @Override
    public ClientConnection getConnection() {
        return connection;
    }

    @Override
    public HecatonClientInstanceImpl getClient() {
        return client;
    }
}
