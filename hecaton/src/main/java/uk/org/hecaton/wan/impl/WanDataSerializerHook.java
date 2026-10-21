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
import uk.org.hecaton.internal.serialization.impl.ArrayDataSerializableFactory;
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
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;

import java.util.function.Supplier;

import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.WAN_REPLICATION_DS_FACTORY;
import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.WAN_REPLICATION_DS_FACTORY_ID;

/**
 * {@link uk.org.hecaton.internal.serialization.DataSerializerHook} implementation for Wan Replication classes
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
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

    private static final int LEN = WAN_SYNC_OPERATION + 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        //noinspection unchecked
        Supplier<IdentifiedDataSerializable>[] constructors = new Supplier[LEN];
        constructors[MAP_REPLICATION_UPDATE] = WanMapAddOrUpdateEvent::new;
        constructors[MAP_REPLICATION_REMOVE] = WanMapRemoveEvent::new;
        constructors[WAN_MAP_ENTRY_VIEW] = WanMapEntryView::new;
        constructors[WAN_CACHE_ENTRY_VIEW] = WanCacheEntryView::new;
        constructors[WAN_EVENT_CONTAINER_REPLICATION_OPERATION] = WanEventContainerReplicationOperation::new;
        constructors[WAN_BATCH_OPERATION] = WanBatchOperation::new;
        constructors[WAN_QUEUE_REMOVE_OPERATION] = WanQueueRemoveOperation::new;
        constructors[WAN_PARTITION_EVENTS] = WanPartitionEvents::new;
        constructors[CACHE_REPLICATION_UPDATE] = WanCacheUpdateEvent::new;
        constructors[CACHE_REPLICATION_REMOVE] = WanCacheRemoveEvent::new;
        constructors[ADD_WAN_CONFIG_OPERATION] = AddWanConfigOperation::new;
        constructors[MERKLE_TREE_LEAVES_OPERATION] = MerkleTreeLeavesOperation::new;
        constructors[WAN_MERKLE_TREE_QUERY_OPERATION] = WanMerkleTreeQueryOperation::new;
        constructors[WAN_SYNC_OPERATION] = WanSyncOperation::new;
        return new ArrayDataSerializableFactory(constructors);
    }
}
