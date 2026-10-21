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
package uk.org.hecaton.cp.internal.datastructures.cpmap.proxy;

import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.CPMap;
import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.cp.internal.RaftInvocationManager;
import uk.org.hecaton.cp.internal.RaftService;
import uk.org.hecaton.cp.internal.datastructures.cpmap.CPMapService;
import uk.org.hecaton.cp.internal.datastructures.cpmap.operation.CompareAndSetOp;
import uk.org.hecaton.cp.internal.datastructures.cpmap.operation.GetOp;
import uk.org.hecaton.cp.internal.datastructures.cpmap.operation.PutOp;
import uk.org.hecaton.cp.internal.datastructures.cpmap.operation.RemoveOp;
import uk.org.hecaton.cp.internal.datastructures.spi.operation.DestroyRaftObjectOp;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.spi.impl.NodeEngine;

import javax.annotation.Nonnull;

import static uk.org.hecaton.cp.internal.raft.QueryPolicy.LINEARIZABLE;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;

/**
 * Member-side proxy of a {@link CPMap}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class CPMapProxy<K, V> implements CPMap<K, V> {

    private final RaftInvocationManager invocationManager;
    private final SerializationService serializationService;
    private final RaftGroupId groupId;
    private final String proxyName;
    private final String objectName;

    public CPMapProxy(NodeEngine nodeEngine, RaftGroupId groupId, String proxyName, String objectName) {
        RaftService service = nodeEngine.getService(RaftService.SERVICE_NAME);
        this.invocationManager = service.getInvocationManager();
        this.serializationService = nodeEngine.getSerializationService();
        this.groupId = groupId;
        this.proxyName = proxyName;
        this.objectName = objectName;
    }

    @Override
    public V put(@Nonnull K key, @Nonnull V value) {
        return invocationManager.<V>invoke(groupId, putOp(key, value, PutOp.Mode.PUT)).joinInternal();
    }

    @Override
    public V putIfAbsent(@Nonnull K key, @Nonnull V value) {
        return invocationManager.<V>invoke(groupId, putOp(key, value, PutOp.Mode.PUT_IF_ABSENT)).joinInternal();
    }

    @Override
    public void set(@Nonnull K key, @Nonnull V value) {
        invocationManager.invoke(groupId, putOp(key, value, PutOp.Mode.SET)).joinInternal();
    }

    @Override
    public V remove(@Nonnull K key) {
        return invocationManager.<V>invoke(groupId, new RemoveOp(objectName, keyData(key), true)).joinInternal();
    }

    @Override
    public void delete(@Nonnull K key) {
        invocationManager.invoke(groupId, new RemoveOp(objectName, keyData(key), false)).joinInternal();
    }

    @Override
    public boolean compareAndSet(@Nonnull K key, @Nonnull V expectedValue, @Nonnull V newValue) {
        checkNotNull(expectedValue, "Expected value cannot be null");
        checkNotNull(newValue, "New value cannot be null");
        CompareAndSetOp op = new CompareAndSetOp(objectName, keyData(key), toData(expectedValue), toData(newValue));
        return invocationManager.<Boolean>invoke(groupId, op).joinInternal();
    }

    @Override
    public V get(@Nonnull K key) {
        return invocationManager.<V>query(groupId, new GetOp(objectName, keyData(key)), LINEARIZABLE).joinInternal();
    }

    private PutOp putOp(K key, V value, PutOp.Mode mode) {
        Data keyData = keyData(key);
        checkNotNull(value, "Value cannot be null");
        return new PutOp(objectName, keyData, toData(value), mode);
    }

    private Data keyData(K key) {
        checkNotNull(key, "Key cannot be null");
        return toData(key);
    }

    private Data toData(Object value) {
        return serializationService.toData(value);
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return proxyName;
    }

    @Override
    public String getServiceName() {
        return CPMapService.SERVICE_NAME;
    }

    @Override
    public void destroy() {
        invocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), objectName)).joinInternal();
    }

    public CPGroupId getGroupId() {
        return groupId;
    }
}
