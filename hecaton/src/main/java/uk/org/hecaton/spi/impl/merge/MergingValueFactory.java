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

package uk.org.hecaton.spi.impl.merge;

import uk.org.hecaton.cache.impl.record.CacheRecord;
import uk.org.hecaton.cache.impl.wan.WanCacheEntryView;
import uk.org.hecaton.cardinality.impl.hyperloglog.HyperLogLog;
import uk.org.hecaton.collection.impl.collection.CollectionItem;
import uk.org.hecaton.collection.impl.queue.QueueItem;
import uk.org.hecaton.core.EntryView;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.recordstore.expiry.ExpiryMetadata;
import uk.org.hecaton.map.impl.wan.WanMapEntryView;
import uk.org.hecaton.multimap.impl.MultiMapContainer;
import uk.org.hecaton.multimap.impl.MultiMapMergeContainer;
import uk.org.hecaton.multimap.impl.MultiMapRecord;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedRecord;
import uk.org.hecaton.ringbuffer.impl.Ringbuffer;
import uk.org.hecaton.scheduledexecutor.impl.ScheduledTaskDescriptor;
import uk.org.hecaton.spi.merge.MergingEntry;
import uk.org.hecaton.spi.merge.MergingValue;
import uk.org.hecaton.spi.merge.RingbufferMergeData;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.AtomicLongMergeTypes;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.AtomicReferenceMergeTypes;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.CardinalityEstimatorMergeTypes;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.CollectionMergeTypes;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.MultiMapMergeTypes;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.QueueMergeTypes;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.ReplicatedMapMergeTypes;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.RingbufferMergeTypes;
import uk.org.hecaton.spi.merge.SplitBrainMergeTypes.ScheduledExecutorMergeTypes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Queue;

/**
 * Provides static factory methods to create {@link MergingValue} and {@link MergingEntry} instances.
 *
 * @since 3.10
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public final class MergingValueFactory {

    private MergingValueFactory() {
    }

    public static <V> CollectionMergeTypes<V> createMergingValue(SerializationService serializationService,
                                                                 Collection<CollectionItem> items) {
        Collection<Object> values = new ArrayList<>(items.size());
        for (CollectionItem item : items) {
            values.add(item.getValue());
        }
        return new CollectionMergingValueImpl<V>(serializationService)
                .setValue(values);
    }

    public static <V> QueueMergeTypes<V> createMergingValue(SerializationService serializationService, Queue<QueueItem> items) {
        Collection<Object> values = new ArrayList<>(items.size());
        for (QueueItem item : items) {
            values.add(item.getSerializedObject());
        }
        return new QueueMergingValueImpl<V>(serializationService)
                .setValue(values);
    }

    public static AtomicLongMergeTypes createMergingValue(SerializationService serializationService, Long value) {
        return new AtomicLongMergingValueImpl(serializationService)
                .setValue(value);
    }

    public static AtomicReferenceMergeTypes createMergingValue(SerializationService serializationService, Data value) {
        return new AtomicReferenceMergingValueImpl(serializationService)
                .setValue(value);
    }

    public static <K, V> MapMergeTypes<K, V> createMergingEntry(SerializationService serializationService,
                                                                EntryView<Data, Data> entryView) {
        return new MapMergingEntryImpl<K, V>(serializationService)
                .setKey(entryView.getKey())
                .setValue(entryView.getValue())
                .setCreationTime(entryView.getCreationTime())
                .setExpirationTime(entryView.getExpirationTime())
                .setLastStoredTime(entryView.getLastStoredTime())
                .setLastUpdateTime(entryView.getLastUpdateTime())
                .setLastAccessTime(entryView.getLastAccessTime())
                .setHits(entryView.getHits())
                .setTtl(entryView.getTtl())
                .setMaxIdle(entryView.getMaxIdle())
                .setVersion(entryView.getVersion())
                .setCost(entryView.getCost());
    }

    public static MapMergingEntryImpl<Object, Object> createMergingEntry(SerializationService serializationService,
                                                                         WanMapEntryView<Object, Object> entryView) {
        return new MapMergingEntryImpl<>(serializationService)
                .setKey(entryView.getDataKey())
                .setValue(entryView.getDataValue())
                .setCreationTime(entryView.getCreationTime())
                .setExpirationTime(entryView.getExpirationTime())
                .setLastStoredTime(entryView.getLastStoredTime())
                .setLastUpdateTime(entryView.getLastUpdateTime())
                .setLastAccessTime(entryView.getLastAccessTime())
                .setHits(entryView.getHits())
                .setTtl(entryView.getTtl())
                .setMaxIdle(entryView.getMaxIdle())
                .setVersion(entryView.getVersion())
                .setCost(entryView.getCost());
    }

    public static <K, V> MapMergeTypes<K, V> createMergingEntry(SerializationService serializationService,
                                                                Data dataKey,
                                                                Record record,
                                                                ExpiryMetadata expiryMetadata) {
        return new MapMergingEntryImpl<K, V>(serializationService)
                .setKey(dataKey)
                .setValue(serializationService.toData(record.getValue()))
                .setCreationTime(record.getCreationTime())
                .setLastStoredTime(record.getLastStoredTime())
                .setLastAccessTime(record.getLastAccessTime())
                .setLastStoredTime(record.getLastStoredTime())
                .setLastUpdateTime(record.getLastUpdateTime())
                .setHits(record.getHits())
                .setVersion(record.getVersion())
                .setCost(record.getCost())
                .setTtl(expiryMetadata.getTtl())
                .setMaxIdle(expiryMetadata.getMaxIdle())
                .setExpirationTime(expiryMetadata.getExpirationTime());
    }

    public static <K, V> MapMergeTypes<K, V> createMergingEntry(SerializationService serializationService,
                                                                Data dataKey,
                                                                Data dataValue,
                                                                Record record,
                                                                ExpiryMetadata expiryMetadata) {
        return new MapMergingEntryImpl<K, V>(serializationService)
                .setKey(dataKey)
                .setValue(dataValue)
                .setCreationTime(record.getCreationTime())
                .setLastStoredTime(record.getLastStoredTime())
                .setLastAccessTime(record.getLastAccessTime())
                .setLastUpdateTime(record.getLastUpdateTime())
                .setHits(record.getHits())
                .setVersion(record.getVersion())
                .setCost(record.getCost())
                .setTtl(expiryMetadata.getTtl())
                .setMaxIdle(expiryMetadata.getMaxIdle())
                .setExpirationTime(expiryMetadata.getExpirationTime());
    }

    public static <K, V> CacheMergeTypes<K, V> createMergingEntry(SerializationService serializationService,
                                                                  WanCacheEntryView<Object, Object> entryView) {
        return new CacheMergingEntryImpl<K, V>(serializationService)
                .setKey(entryView.getDataKey())
                .setValue(entryView.getDataValue())
                .setCreationTime(entryView.getCreationTime())
                .setExpirationTime(entryView.getExpirationTime())
                .setLastAccessTime(entryView.getLastAccessTime())
                .setHits(entryView.getHits());
    }

    public static <K, V, R extends CacheRecord> CacheMergeTypes<K, V> createMergingEntry(
            SerializationService serializationService, Data key, Data value, R record) {
        return new CacheMergingEntryImpl<K, V>(serializationService)
                .setKey(key)
                .setValue(value)
                .setCreationTime(record.getCreationTime())
                .setExpirationTime(record.getExpirationTime())
                .setLastAccessTime(record.getLastAccessTime())
                .setHits(record.getHits());
    }

    public static <K, V> ReplicatedMapMergeTypes<K, V> createMergingEntry(SerializationService serializationService,
                                                                          ReplicatedRecord record) {
        return new ReplicatedMapMergingEntryImpl<K, V>(serializationService)
                .setKey(record.getKeyInternal())
                .setValue(record.getValueInternal())
                .setCreationTime(record.getCreationTime())
                .setHits(record.getHits())
                .setLastAccessTime(record.getLastAccessTime())
                .setLastUpdateTime(record.getUpdateTime())
                .setTtl(record.getTtlMillis());
    }

    public static <K, V> MultiMapMergeTypes<K, V> createMergingEntry(SerializationService serializationService,
                                                                     MultiMapMergeContainer container) {
        Collection<Object> values = new ArrayList<>(container.getRecords().size());
        for (MultiMapRecord record : container.getRecords()) {
            values.add(record.getObject());
        }

        return new MultiMapMergingEntryImpl<K, V>(serializationService)
                .setKey(container.getKey())
                .setValues(values)
                .setCreationTime(container.getCreationTime())
                .setLastAccessTime(container.getLastAccessTime())
                .setLastUpdateTime(container.getLastUpdateTime())
                .setHits(container.getHits());
    }

    public static <K, V> MultiMapMergeTypes<K, V> createMergingEntry(SerializationService serializationService,
                                                                     MultiMapContainer container, Data dataKey,
                                                                     Collection<MultiMapRecord> records, long hits) {
        Collection<Object> values = new ArrayList<>(records.size());
        for (MultiMapRecord record : records) {
            values.add(record.getObject());
        }

        return new MultiMapMergingEntryImpl<K, V>(serializationService)
                .setKey(dataKey)
                .setValues(values)
                .setCreationTime(container.getCreationTime())
                .setLastAccessTime(container.getLastAccessTime())
                .setLastUpdateTime(container.getLastUpdateTime())
                .setHits(hits);
    }

    public static RingbufferMergeTypes createMergingValue(SerializationService serializationService, Ringbuffer<Object> items) {
        RingbufferMergeData mergingData = new RingbufferMergeData(items);
        return new RingbufferMergingValueImpl(serializationService)
                .setValues(mergingData);
    }

    public static CardinalityEstimatorMergeTypes createMergingEntry(SerializationService serializationService,
                                                                    String name, HyperLogLog hyperLogLog) {
        return new CardinalityEstimatorMergingEntry(serializationService)
                .setKey(name)
                .setValue(hyperLogLog);
    }

    public static ScheduledExecutorMergeTypes createMergingEntry(SerializationService serializationService,
                                                                 ScheduledTaskDescriptor task) {
        return new ScheduledExecutorMergingEntryImpl(serializationService)
                .setKey(task.getDefinition().getName())
                .setValue(task);
    }
}
