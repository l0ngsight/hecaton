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

package uk.org.hecaton.multimap.impl.txn;

import uk.org.hecaton.multimap.impl.MultiMapRecord;
import uk.org.hecaton.multimap.impl.MultiMapService;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.transaction.TransactionException;
import uk.org.hecaton.transaction.impl.Transaction;

import java.util.ArrayList;
import java.util.Collection;

public class TransactionalMultiMapProxy<K, V> extends TransactionalMultiMapProxySupport<K, V> {

    public TransactionalMultiMapProxy(NodeEngine nodeEngine, MultiMapService service, String name, Transaction tx) {
        super(nodeEngine, service, name, tx);
    }

    @Override
    public boolean put(K key, V value) throws TransactionException {
        checkTransactionActive();
        Data dataKey = getNodeEngine().toData(key);
        Data dataValue = getNodeEngine().toData(value);
        return putInternal(dataKey, dataValue);
    }

    @Override
    public Collection<V> get(K key) {
        checkTransactionActive();
        Data dataKey = getNodeEngine().toData(key);
        Collection<MultiMapRecord> coll = getInternal(dataKey);
        Collection<V> collection = new ArrayList<>(coll.size());
        for (MultiMapRecord record : coll) {
            collection.add((V) toObjectIfNeeded(record.getObject()));
        }
        return collection;
    }

    @Override
    public boolean remove(Object key, Object value) {
        checkTransactionActive();
        Data dataKey = getNodeEngine().toData(key);
        Data dataValue = getNodeEngine().toData(value);
        return removeInternal(dataKey, dataValue);
    }

    @Override
    public Collection<V> remove(Object key) {
        checkTransactionActive();
        Data dataKey = getNodeEngine().toData(key);
        Collection<MultiMapRecord> coll = removeAllInternal(dataKey);
        Collection<V> result = new ArrayList<>(coll.size());
        for (MultiMapRecord record : coll) {
            result.add((V) toObjectIfNeeded(record.getObject()));
        }
        return result;
    }

    @Override
    public int valueCount(K key) {
        checkTransactionActive();
        Data dataKey = getNodeEngine().toData(key);
        return valueCountInternal(dataKey);
    }

    @Override
    public String toString() {
        return "TransactionalMultiMap{name=" + getName() + '}';
    }
}
