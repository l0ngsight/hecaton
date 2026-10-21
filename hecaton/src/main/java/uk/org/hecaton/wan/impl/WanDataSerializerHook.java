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

package uk.org.hecaton.wan.impl;

import uk.org.hecaton.cache.impl.wan.WanCacheEntryView;
import uk.org.hecaton.cache.impl.wan.WanCacheRemoveEvent;
import uk.org.hecaton.cache.impl.wan.WanCacheUpdateEvent;
import uk.org.hecaton.internal.serialization.DataSerializerHook;
import uk.org.hecaton.internal.serialization.impl.FactoryIdHelper;
import uk.org.hecaton.map.impl.wan.WanMapRemoveEvent;
import uk.org.hecaton.map.impl.wan.WanMapAddOrUpdateEvent;
import uk.org.hecaton.wan.impl.batch.MerkleTreeLeavesOperation;
import uk.org.hecaton.wan.impl.batch.WanBatchOperation;
import uk.org.hecaton.wan.impl.batch.WanMerkleTreeQueryOperation;
import uk.org.hecaton.wan.impl.batch.WanPartitionEvents;
import uk.org.hecaton.wan.impl.batch.WanQueueRemoveOperation;
import uk.org.hecaton.wan.impl.batch.WanSyncOperation;
import uk.org.hecaton.map.impl.wan.WanMapEntryView;
import uk.org.hecaton.nio.serialization.DataSerializableFactory;

import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.WAN_REPLICATION_DS_FACTORY;
import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.WAN_REPLICATION_DS_FACTORY_ID;

/**
 * {@link uk.org.hecaton.internal.serialization.DataSerializerHook} implementation for Wan Replication classes
 */
public class WanDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(WAN_REPLICATION_DS_FACTORY, WAN_REPLICATION_DS_FACTORY_ID);

    public static final int MAP_REPLICATION_UPDATE = 0;
    public static final int MAP_REPLICATION_REMOVE = 1;
    public static final int WAN_MAP_ENTRY_VIEW = 2;
    public static final int WAN_CACHE_ENTRY_VIEW = 3;
    public static final int WAN_EVENT_CONTAINER_REPLICATION_OPERATION = 4;
    public static final int WAN_BATCH_OPERATION = 5;
    public static final int WAN_QUEUE_REMOVE_OPERATION = 6;
    public static final int WAN_PARTITION_EVENTS = 7;
    public static final int CACHE_REPLICATION_UPDATE = 8;
    public static final int CACHE_REPLICATION_REMOVE = 9;
    public static final int ADD_WAN_CONFIG_OPERATION = 10;
    public static final int MERKLE_TREE_LEAVES_OPERATION = 11;
    public static final int WAN_MERKLE_TREE_QUERY_OPERATION = 12;
    public static final int WAN_SYNC_OPERATION = 13;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> switch (typeId) {
            case MAP_REPLICATION_UPDATE -> new WanMapAddOrUpdateEvent();
            case MAP_REPLICATION_REMOVE -> new WanMapRemoveEvent();
            case WAN_MAP_ENTRY_VIEW -> new WanMapEntryView<>();
            case WAN_CACHE_ENTRY_VIEW -> new WanCacheEntryView<>();
            case WAN_EVENT_CONTAINER_REPLICATION_OPERATION -> new WanEventContainerReplicationOperation();
            case WAN_BATCH_OPERATION -> new WanBatchOperation();
            case WAN_QUEUE_REMOVE_OPERATION -> new WanQueueRemoveOperation();
            case WAN_PARTITION_EVENTS -> new WanPartitionEvents();
            case CACHE_REPLICATION_UPDATE -> new WanCacheUpdateEvent();
            case CACHE_REPLICATION_REMOVE -> new WanCacheRemoveEvent();
            case ADD_WAN_CONFIG_OPERATION -> new AddWanConfigOperation();
            case MERKLE_TREE_LEAVES_OPERATION -> new MerkleTreeLeavesOperation();
            case WAN_MERKLE_TREE_QUERY_OPERATION -> new WanMerkleTreeQueryOperation();
            case WAN_SYNC_OPERATION -> new WanSyncOperation();
            default -> throw new IllegalArgumentException("Unknown type-id: " + typeId);
        };
    }
}
