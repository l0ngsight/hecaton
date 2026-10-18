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

package uk.org.hecaton.collection.impl.txncollection;

import uk.org.hecaton.collection.impl.collection.CollectionItem;
import uk.org.hecaton.collection.impl.collection.operations.CollectionSizeOperation;
import uk.org.hecaton.collection.impl.txncollection.operations.CollectionReserveAddOperation;
import uk.org.hecaton.collection.impl.txncollection.operations.CollectionReserveRemoveOperation;
import uk.org.hecaton.collection.impl.txncollection.operations.CollectionTxnAddOperation;
import uk.org.hecaton.collection.impl.txncollection.operations.CollectionTxnRemoveOperation;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.internal.services.RemoteService;
import uk.org.hecaton.spi.impl.TransactionalDistributedObject;
import uk.org.hecaton.transaction.TransactionException;
import uk.org.hecaton.transaction.TransactionNotActiveException;
import uk.org.hecaton.transaction.impl.Transaction;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Future;

import static uk.org.hecaton.collection.impl.collection.CollectionContainer.INVALID_ITEM_ID;
import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;

public abstract class AbstractTransactionalCollectionProxy<S extends RemoteService, E>
        extends TransactionalDistributedObject<S> {

    protected final Set<Long> itemIdSet = new HashSet<>();
    protected final String name;
    protected final int partitionId;
    protected final OperationService operationService;

    public AbstractTransactionalCollectionProxy(String name, Transaction tx, NodeEngine nodeEngine, S service) {
        super(nodeEngine, service, tx);
        this.name = name;
        this.partitionId = nodeEngine.getPartitionService().getPartitionId(getNameAsPartitionAwareData());
        this.operationService = nodeEngine.getOperationService();
    }

    protected abstract Collection<CollectionItem> getCollection();

    @Override
    public String getName() {
        return name;
    }

    public boolean add(E e) {
        checkTransactionActive();
        checkObjectNotNull(e);

        Data value = getNodeEngine().toData(e);
        CollectionReserveAddOperation operation = new CollectionReserveAddOperation(name, tx.getTxnId(), null);
        try {
            Future<Long> future = operationService.invokeOnPartition(getServiceName(), operation, partitionId);
            Long itemId = future.get();
            if (itemId != null) {
                if (!itemIdSet.add(itemId)) {
                    throw new TransactionException("Duplicate itemId: " + itemId);
                }
                getCollection().add(new CollectionItem(itemId, value));
                CollectionTxnAddOperation op = new CollectionTxnAddOperation(name, itemId, value);
                putToRecord(op);
                return true;
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
        return false;
    }

    protected void putToRecord(CollectionTxnOperation operation) {
        CollectionTransactionLogRecord logRecord = (CollectionTransactionLogRecord) tx.get(name);
        if (logRecord == null) {
            logRecord = new CollectionTransactionLogRecord(getServiceName(), tx.getTxnId(), name, partitionId);
            tx.add(logRecord);
        }
        logRecord.addOperation(operation);
    }

    private void removeFromRecord(long itemId) {
        CollectionTransactionLogRecord logRecord = (CollectionTransactionLogRecord) tx.get(name);
        int size = logRecord.removeOperation(itemId);
        if (size == 0) {
            tx.remove(name);
        }
    }

    public boolean remove(E e) {
        checkTransactionActive();
        checkObjectNotNull(e);

        Data value = getNodeEngine().toData(e);
        Iterator<CollectionItem> iterator = getCollection().iterator();
        long reservedItemId = INVALID_ITEM_ID;
        while (iterator.hasNext()) {
            CollectionItem item = iterator.next();
            if (value.equals(item.getValue())) {
                reservedItemId = item.getItemId();
                break;
            }
        }
        CollectionReserveRemoveOperation operation = new CollectionReserveRemoveOperation(
                name, reservedItemId, value, tx.getTxnId());
        try {
            Future<CollectionItem> future = operationService.invokeOnPartition(getServiceName(), operation, partitionId);
            CollectionItem item = future.get();
            if (item != null) {
                if (reservedItemId == item.getItemId()) {
                    iterator.remove();
                    removeFromRecord(reservedItemId);
                    itemIdSet.remove(reservedItemId);
                    return true;
                }
                if (!itemIdSet.add(item.getItemId())) {
                    throw new TransactionException("Duplicate itemId: " + item.getItemId());
                }
                CollectionTxnRemoveOperation op = new CollectionTxnRemoveOperation(name, item.getItemId());
                putToRecord(op);
                return true;
            }
        } catch (Throwable t) {
            throw rethrow(t);
        }
        return false;
    }

    public int size() {
        checkTransactionActive();

        try {
            CollectionSizeOperation operation = new CollectionSizeOperation(name);
            Future<Integer> future = operationService.invokeOnPartition(getServiceName(), operation, partitionId);
            Integer size = future.get();
            return size + getCollection().size();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected void checkTransactionActive() {
        if (tx.getState() != Transaction.State.ACTIVE) {
            throw new TransactionNotActiveException("Transaction is not active!");
        }
    }

    protected void checkObjectNotNull(Object o) {
        checkNotNull(o, "Object is null");
    }
}
