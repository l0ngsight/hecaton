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

package uk.org.hecaton.cache.impl;

import uk.org.hecaton.cache.impl.operation.CacheClearOperationFactory;
import uk.org.hecaton.cache.impl.operation.CacheContainsKeyOperation;
import uk.org.hecaton.cache.impl.operation.CacheEntryProcessorOperation;
import uk.org.hecaton.cache.impl.operation.CacheFetchEntriesOperation;
import uk.org.hecaton.cache.impl.operation.CacheFetchKeysOperation;
import uk.org.hecaton.cache.impl.operation.CacheGetAllOperationFactory;
import uk.org.hecaton.cache.impl.operation.CacheGetAndRemoveOperation;
import uk.org.hecaton.cache.impl.operation.CacheGetAndReplaceOperation;
import uk.org.hecaton.cache.impl.operation.CacheGetOperation;
import uk.org.hecaton.cache.impl.operation.CacheLoadAllOperationFactory;
import uk.org.hecaton.cache.impl.operation.CacheMergeOperation;
import uk.org.hecaton.cache.impl.operation.CacheMergeOperationFactory;
import uk.org.hecaton.cache.impl.operation.CachePutAllOperation;
import uk.org.hecaton.cache.impl.operation.CachePutIfAbsentOperation;
import uk.org.hecaton.cache.impl.operation.CachePutOperation;
import uk.org.hecaton.cache.impl.operation.CacheRemoveAllOperationFactory;
import uk.org.hecaton.cache.impl.operation.CacheRemoveOperation;
import uk.org.hecaton.cache.impl.operation.CacheReplaceOperation;
import uk.org.hecaton.cache.impl.operation.CacheSetExpiryPolicyOperation;
import uk.org.hecaton.cache.impl.operation.CacheSizeOperationFactory;
import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.internal.iteration.IterationPointer;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationFactory;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicy;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Provide operations other then {@link InMemoryFormat#NATIVE}
 */
@SuppressWarnings("ClassDataAbstractionCoupling")
public class DefaultOperationProvider implements CacheOperationProvider {

    protected final String nameWithPrefix;

    public DefaultOperationProvider(String nameWithPrefix) {
        this.nameWithPrefix = nameWithPrefix;
    }

    @Override
    public Operation createPutOperation(Data key, Data value, ExpiryPolicy policy, boolean get, int completionId) {
        return new CachePutOperation(nameWithPrefix, key, value, policy, get, completionId);
    }

    @Override
    public Operation createPutAllOperation(List<Map.Entry<Data, Data>> entries, ExpiryPolicy policy, int completionId) {
        return new CachePutAllOperation(nameWithPrefix, entries, policy, completionId);
    }

    @Override
    public Operation createGetOperation(Data key, ExpiryPolicy policy) {
        return new CacheGetOperation(nameWithPrefix, key, policy);
    }

    @Override
    public Operation createContainsKeyOperation(Data key) {
        return new CacheContainsKeyOperation(nameWithPrefix, key);
    }

    @Override
    public Operation createPutIfAbsentOperation(Data key, Data value, ExpiryPolicy policy, int completionId) {
        return new CachePutIfAbsentOperation(nameWithPrefix, key, value, policy, completionId);
    }

    @Override
    public Operation createRemoveOperation(Data key, Data oldValue, int completionId) {
        return new CacheRemoveOperation(nameWithPrefix, key, oldValue, completionId);
    }

    @Override
    public Operation createGetAndRemoveOperation(Data key, int completionId) {
        return new CacheGetAndRemoveOperation(nameWithPrefix, key, completionId);
    }

    @Override
    public Operation createReplaceOperation(Data key, Data oldValue, Data newValue, ExpiryPolicy policy, int completionId) {
        return new CacheReplaceOperation(nameWithPrefix, key, oldValue, newValue, policy, completionId);
    }

    @Override
    public Operation createGetAndReplaceOperation(Data key, Data value, ExpiryPolicy policy, int completionId) {
        return new CacheGetAndReplaceOperation(nameWithPrefix, key, value, policy, completionId);
    }

    @Override
    public Operation createEntryProcessorOperation(Data key, Integer completionId, EntryProcessor
            entryProcessor, Object... arguments) {
        return new CacheEntryProcessorOperation(nameWithPrefix, key, completionId, entryProcessor, arguments);
    }

    @Override
    public Operation createFetchKeysOperation(IterationPointer[] pointers, int fetchSize) {
        return new CacheFetchKeysOperation(nameWithPrefix, pointers, fetchSize);
    }

    @Override
    public Operation createFetchEntriesOperation(IterationPointer[] pointers, int fetchSize) {
        return new CacheFetchEntriesOperation(nameWithPrefix, pointers, fetchSize);
    }

    @Override
    public Operation createMergeOperation(String name, List<CacheMergeTypes<Object, Object>> mergingEntries,
                                          SplitBrainMergePolicy<Object, CacheMergeTypes<Object, Object>, Object> policy) {
        return new CacheMergeOperation(name, mergingEntries, policy);
    }

    @Override
    public OperationFactory createMergeOperationFactory(String name, int[] partitions,
                                                        List<CacheMergeTypes<Object, Object>>[] mergingEntries,
                                                        SplitBrainMergePolicy<Object, CacheMergeTypes<Object, Object>,
                                                                Object> policy) {
        return new CacheMergeOperationFactory(name, partitions, mergingEntries, policy);
    }

    @Override
    public Operation createSetExpiryPolicyOperation(List<Data> keys, Data expiryPolicy) {
        return new CacheSetExpiryPolicyOperation(nameWithPrefix, keys, expiryPolicy);
    }

    @Override
    public OperationFactory createGetAllOperationFactory(Set<Data> keySet, ExpiryPolicy policy) {
        return new CacheGetAllOperationFactory(nameWithPrefix, keySet, policy);
    }

    @Override
    public OperationFactory createLoadAllOperationFactory(Set<Data> keySet, boolean replaceExistingValues) {
        return new CacheLoadAllOperationFactory(nameWithPrefix, keySet, replaceExistingValues);
    }

    @Override
    public OperationFactory createClearOperationFactory() {
        return new CacheClearOperationFactory(nameWithPrefix);
    }

    @Override
    public OperationFactory createRemoveAllOperationFactory(Set<Data> keySet, Integer completionId) {
        return new CacheRemoveAllOperationFactory(nameWithPrefix, keySet, completionId);
    }

    @Override
    public OperationFactory createSizeOperationFactory() {
        return new CacheSizeOperationFactory(nameWithPrefix);
    }
}
