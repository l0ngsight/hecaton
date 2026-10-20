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

import uk.org.hecaton.cache.HecatonExpiryPolicy;
import uk.org.hecaton.cache.impl.event.CachePartitionLostEventFilter;
import uk.org.hecaton.cache.impl.journal.CacheEventJournalReadOperation;
import uk.org.hecaton.cache.impl.journal.CacheEventJournalReadResultSetImpl;
import uk.org.hecaton.cache.impl.journal.CacheEventJournalSubscribeOperation;
import uk.org.hecaton.cache.impl.journal.DeserializingEventJournalCacheEvent;
import uk.org.hecaton.cache.impl.journal.InternalEventJournalCacheEvent;
import uk.org.hecaton.cache.impl.merge.entry.DefaultCacheEntryView;
import uk.org.hecaton.cache.impl.operation.AddCacheConfigOperation;
import uk.org.hecaton.cache.impl.operation.CacheBackupEntryProcessorOperation;
import uk.org.hecaton.cache.impl.operation.CacheClearBackupOperation;
import uk.org.hecaton.cache.impl.operation.CacheClearOperation;
import uk.org.hecaton.cache.impl.operation.CacheClearOperationFactory;
import uk.org.hecaton.cache.impl.operation.CacheContainsKeyOperation;
import uk.org.hecaton.cache.impl.operation.CacheDestroyOperation;
import uk.org.hecaton.cache.impl.operation.CacheEntryProcessorOperation;
import uk.org.hecaton.cache.impl.operation.CacheExpireBatchBackupOperation;
import uk.org.hecaton.cache.impl.operation.CacheFetchEntriesOperation;
import uk.org.hecaton.cache.impl.operation.CacheFetchKeysOperation;
import uk.org.hecaton.cache.impl.operation.CacheGetAllOperation;
import uk.org.hecaton.cache.impl.operation.CacheGetAllOperationFactory;
import uk.org.hecaton.cache.impl.operation.CacheGetAndRemoveOperation;
import uk.org.hecaton.cache.impl.operation.CacheGetAndReplaceOperation;
import uk.org.hecaton.cache.impl.operation.CacheGetConfigOperation;
import uk.org.hecaton.cache.impl.operation.CacheGetInvalidationMetaDataOperation;
import uk.org.hecaton.cache.impl.operation.CacheGetInvalidationMetaDataOperation.MetaDataResponse;
import uk.org.hecaton.cache.impl.operation.CacheGetOperation;
import uk.org.hecaton.cache.impl.operation.CacheListenerRegistrationOperation;
import uk.org.hecaton.cache.impl.operation.CacheLoadAllOperation;
import uk.org.hecaton.cache.impl.operation.CacheLoadAllOperationFactory;
import uk.org.hecaton.cache.impl.operation.CacheManagementConfigOperation;
import uk.org.hecaton.cache.impl.operation.CacheMergeOperation;
import uk.org.hecaton.cache.impl.operation.CacheMergeOperationFactory;
import uk.org.hecaton.cache.impl.operation.CacheNearCacheStateHolder;
import uk.org.hecaton.cache.impl.operation.CachePutAllBackupOperation;
import uk.org.hecaton.cache.impl.operation.CachePutAllOperation;
import uk.org.hecaton.cache.impl.operation.CachePutBackupOperation;
import uk.org.hecaton.cache.impl.operation.CachePutIfAbsentOperation;
import uk.org.hecaton.cache.impl.operation.CachePutOperation;
import uk.org.hecaton.cache.impl.operation.CacheRemoveAllBackupOperation;
import uk.org.hecaton.cache.impl.operation.CacheRemoveAllOperation;
import uk.org.hecaton.cache.impl.operation.CacheRemoveAllOperationFactory;
import uk.org.hecaton.cache.impl.operation.CacheRemoveBackupOperation;
import uk.org.hecaton.cache.impl.operation.CacheRemoveOperation;
import uk.org.hecaton.cache.impl.operation.CacheReplaceOperation;
import uk.org.hecaton.cache.impl.operation.CacheReplicationOperation;
import uk.org.hecaton.cache.impl.operation.CacheSetExpiryPolicyBackupOperation;
import uk.org.hecaton.cache.impl.operation.CacheSetExpiryPolicyOperation;
import uk.org.hecaton.cache.impl.operation.CacheSizeOperation;
import uk.org.hecaton.cache.impl.operation.CacheSizeOperationFactory;
import uk.org.hecaton.cache.impl.operation.OnJoinCacheOperation;
import uk.org.hecaton.cache.impl.record.CacheDataRecord;
import uk.org.hecaton.cache.impl.record.CacheObjectRecord;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheAssignAndGetUuidsOperation;
import uk.org.hecaton.client.impl.protocol.task.cache.CacheAssignAndGetUuidsOperationFactory;
import uk.org.hecaton.config.CacheConfig;
import uk.org.hecaton.internal.management.operation.GetCacheEntryViewEntryProcessor;
import uk.org.hecaton.internal.management.operation.GetCacheEntryViewEntryProcessor.CacheBrowserEntryView;
import uk.org.hecaton.internal.serialization.DataSerializerHook;
import uk.org.hecaton.internal.serialization.impl.ArrayDataSerializableFactory;
import uk.org.hecaton.internal.serialization.impl.FactoryIdHelper;
import uk.org.hecaton.nio.serialization.DataSerializableFactory;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;

import java.util.function.Supplier;

import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.CACHE_DS_FACTORY;
import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.CACHE_DS_FACTORY_ID;

/**
 * {@link CacheDataSerializerHook} contains all the ID hooks for {@link IdentifiedDataSerializable} classes used
 * inside the JCache framework.
 * <p>CacheProxy operations are mapped here. This factory class is used by internal serialization system to create
 * {@link IdentifiedDataSerializable} classes without using reflection.</p>
 */
@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public final class CacheDataSerializerHook
        implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(CACHE_DS_FACTORY, CACHE_DS_FACTORY_ID);
    public static final short GET = 1;
    public static final short CONTAINS_KEY = 2;
    public static final short PUT = 3;
    public static final short PUT_IF_ABSENT = 4;
    public static final short REMOVE = 5;
    public static final short GET_AND_REMOVE = 6;
    public static final short REPLACE = 7;
    public static final short GET_AND_REPLACE = 8;
    public static final short PUT_BACKUP = 9;
    public static final short PUT_ALL_BACKUP = 10;
    public static final short REMOVE_BACKUP = 11;
    public static final short CLEAR_BACKUP = 12;
    public static final short SIZE = 13;
    public static final short SIZE_FACTORY = 14;
    public static final short CLEAR = 15;
    public static final short CLEAR_FACTORY = 16;
    public static final short GET_ALL = 17;
    public static final short GET_ALL_FACTORY = 18;
    public static final short LOAD_ALL = 19;
    public static final short LOAD_ALL_FACTORY = 20;
    public static final short EXPIRY_POLICY = 21;
    public static final short KEY_ITERATOR = 22;
    public static final short KEY_ITERATION_RESULT = 23;
    public static final short ENTRY_PROCESSOR = 24;
    public static final short CLEAR_RESPONSE = 25;
    public static final short GET_CONFIG = 26;
    public static final short MANAGEMENT_CONFIG = 27;
    public static final short LISTENER_REGISTRATION = 28;
    public static final short DESTROY_CACHE = 29;
    public static final short CACHE_EVENT_DATA = 30;
    public static final short CACHE_EVENT_DATA_SET = 31;
    public static final short BACKUP_ENTRY_PROCESSOR = 32;
    public static final short REMOVE_ALL = 33;
    public static final short REMOVE_ALL_BACKUP = 34;
    public static final short REMOVE_ALL_FACTORY = 35;
    public static final short PUT_ALL = 36;
    public static final short ENTRY_ITERATOR = 37;
    public static final short ENTRY_ITERATION_RESULT = 38;
    public static final short CACHE_PARTITION_LOST_EVENT_FILTER = 39;
    public static final short DEFAULT_CACHE_ENTRY_VIEW = 40;
    public static final short CACHE_REPLICATION = 41;
    public static final short CACHE_POST_JOIN = 42;
    public static final short CACHE_DATA_RECORD = 43;
    public static final short CACHE_OBJECT_RECORD = 44;
    public static final short CACHE_PARTITION_EVENT_DATA = 45;

    public static final short CACHE_INVALIDATION_METADATA = 46;
    public static final short CACHE_INVALIDATION_METADATA_RESPONSE = 47;
    public static final short CACHE_ASSIGN_AND_GET_UUIDS = 48;
    public static final short CACHE_ASSIGN_AND_GET_UUIDS_FACTORY = 49;
    public static final short CACHE_NEAR_CACHE_STATE_HOLDER = 50;
    public static final short CACHE_EVENT_LISTENER_ADAPTOR = 51;
    public static final short EVENT_JOURNAL_SUBSCRIBE_OPERATION = 52;
    public static final short EVENT_JOURNAL_READ_OPERATION = 53;
    public static final short EVENT_JOURNAL_DESERIALIZING_CACHE_EVENT = 54;
    public static final short EVENT_JOURNAL_INTERNAL_CACHE_EVENT = 55;
    public static final short EVENT_JOURNAL_READ_RESULT_SET = 56;
    public static final int PRE_JOIN_CACHE_CONFIG = 57;
    public static final int CACHE_BROWSER_ENTRY_VIEW = 58;
    public static final int GET_CACHE_ENTRY_VIEW_PROCESSOR = 59;

    public static final int MERGE_FACTORY = 60;
    public static final int MERGE = 61;
    public static final int ADD_CACHE_CONFIG_OPERATION = 62;
    public static final int SET_EXPIRY_POLICY = 63;
    public static final int SET_EXPIRY_POLICY_BACKUP = 64;
    public static final int EXPIRE_BATCH_BACKUP = 65;
    public static final int CACHE_CONFIG = 67;

    private static final int LEN = CACHE_CONFIG + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    // squid:S1612 https://github.com/hazelcast/hazelcast/issues/25834
    @SuppressWarnings({"squid:S1612", "MethodLength", "Convert2MethodRef"})
    @Override
    public DataSerializableFactory createFactory() {
        //noinspection unchecked
        Supplier<IdentifiedDataSerializable>[] constructors = new Supplier[LEN];
        constructors[GET] = () -> new CacheGetOperation();
        constructors[CONTAINS_KEY] = () -> new CacheContainsKeyOperation();
        constructors[PUT] = () -> new CachePutOperation();
        constructors[PUT_IF_ABSENT] = () -> new CachePutIfAbsentOperation();
        constructors[REMOVE] = () -> new CacheRemoveOperation();
        constructors[GET_AND_REMOVE] = () -> new CacheGetAndRemoveOperation();
        constructors[REPLACE] = () -> new CacheReplaceOperation();
        constructors[GET_AND_REPLACE] = () -> new CacheGetAndReplaceOperation();
        constructors[PUT_BACKUP] = () -> new CachePutBackupOperation();
        constructors[PUT_ALL_BACKUP] = () -> new CachePutAllBackupOperation();
        constructors[REMOVE_BACKUP] = () -> new CacheRemoveBackupOperation();
        constructors[SIZE] = () -> new CacheSizeOperation();
        constructors[SIZE_FACTORY] = () -> new CacheSizeOperationFactory();
        constructors[CLEAR_FACTORY] = () -> new CacheClearOperationFactory();
        constructors[GET_ALL] = () -> new CacheGetAllOperation();
        constructors[GET_ALL_FACTORY] = () -> new CacheGetAllOperationFactory();
        constructors[LOAD_ALL] = () -> new CacheLoadAllOperation();
        constructors[LOAD_ALL_FACTORY] = () -> new CacheLoadAllOperationFactory();
        constructors[EXPIRY_POLICY] = () -> new HecatonExpiryPolicy();
        constructors[KEY_ITERATOR] = () -> new CacheFetchKeysOperation();
        constructors[KEY_ITERATION_RESULT] = () -> new CacheKeysWithCursor();
        constructors[ENTRY_PROCESSOR] = () -> new CacheEntryProcessorOperation();
        constructors[CLEAR_RESPONSE] = () -> new CacheClearResponse();
        constructors[GET_CONFIG] = () -> new CacheGetConfigOperation();
        constructors[MANAGEMENT_CONFIG] = () -> new CacheManagementConfigOperation();
        constructors[LISTENER_REGISTRATION] = () -> new CacheListenerRegistrationOperation();
        constructors[DESTROY_CACHE] = () -> new CacheDestroyOperation();
        constructors[CACHE_EVENT_DATA] = () -> new CacheEventDataImpl();
        constructors[CACHE_EVENT_DATA_SET] = () -> new CacheEventSet();
        constructors[BACKUP_ENTRY_PROCESSOR] = () -> new CacheBackupEntryProcessorOperation();
        constructors[CLEAR] = () -> new CacheClearOperation();
        constructors[CLEAR_BACKUP] = () -> new CacheClearBackupOperation();
        constructors[REMOVE_ALL] = () -> new CacheRemoveAllOperation();
        constructors[REMOVE_ALL_BACKUP] = () -> new CacheRemoveAllBackupOperation();
        constructors[REMOVE_ALL_FACTORY] = () -> new CacheRemoveAllOperationFactory();
        constructors[PUT_ALL] = () -> new CachePutAllOperation();
        constructors[ENTRY_ITERATOR] = () -> new CacheFetchEntriesOperation();
        constructors[ENTRY_ITERATION_RESULT] = () -> new CacheEntriesWithCursor();
        constructors[CACHE_PARTITION_LOST_EVENT_FILTER] = () -> new CachePartitionLostEventFilter();
        constructors[DEFAULT_CACHE_ENTRY_VIEW] = () -> new DefaultCacheEntryView();
        constructors[CACHE_REPLICATION] = () -> new CacheReplicationOperation();
        constructors[CACHE_POST_JOIN] = () -> new OnJoinCacheOperation();
        constructors[CACHE_DATA_RECORD] = () -> new CacheDataRecord();
        constructors[CACHE_OBJECT_RECORD] = () -> new CacheObjectRecord();
        constructors[CACHE_PARTITION_EVENT_DATA] = () -> new CachePartitionEventData();
        constructors[CACHE_INVALIDATION_METADATA] = () -> new CacheGetInvalidationMetaDataOperation();
        constructors[CACHE_INVALIDATION_METADATA_RESPONSE] = () -> new MetaDataResponse();
        constructors[CACHE_ASSIGN_AND_GET_UUIDS] = () -> new CacheAssignAndGetUuidsOperation();
        constructors[CACHE_ASSIGN_AND_GET_UUIDS_FACTORY] = () -> new CacheAssignAndGetUuidsOperationFactory();
        constructors[CACHE_NEAR_CACHE_STATE_HOLDER] = () -> new CacheNearCacheStateHolder();
        constructors[CACHE_EVENT_LISTENER_ADAPTOR] = () -> new CacheEventListenerAdaptor<>();
        constructors[EVENT_JOURNAL_SUBSCRIBE_OPERATION] = () -> new CacheEventJournalSubscribeOperation();
        constructors[EVENT_JOURNAL_READ_OPERATION] = () -> new CacheEventJournalReadOperation<>();
        constructors[EVENT_JOURNAL_DESERIALIZING_CACHE_EVENT] = () -> new DeserializingEventJournalCacheEvent<>();
        constructors[EVENT_JOURNAL_INTERNAL_CACHE_EVENT] = () -> new InternalEventJournalCacheEvent();
        constructors[EVENT_JOURNAL_READ_RESULT_SET] = () -> new CacheEventJournalReadResultSetImpl<>();
        constructors[PRE_JOIN_CACHE_CONFIG] = () -> new PreJoinCacheConfig<>();
        constructors[CACHE_BROWSER_ENTRY_VIEW] = () -> new CacheBrowserEntryView();
        constructors[GET_CACHE_ENTRY_VIEW_PROCESSOR] = () -> new GetCacheEntryViewEntryProcessor();
        constructors[MERGE_FACTORY] = () -> new CacheMergeOperationFactory();
        constructors[MERGE] = () -> new CacheMergeOperation();
        constructors[ADD_CACHE_CONFIG_OPERATION] = () -> new AddCacheConfigOperation();
        constructors[SET_EXPIRY_POLICY] = () -> new CacheSetExpiryPolicyOperation();
        constructors[SET_EXPIRY_POLICY_BACKUP] = () -> new CacheSetExpiryPolicyBackupOperation();
        constructors[EXPIRE_BATCH_BACKUP] = () -> new CacheExpireBatchBackupOperation();
        constructors[CACHE_CONFIG] = () -> new CacheConfig<>();

        return new ArrayDataSerializableFactory(constructors);
    }
}
