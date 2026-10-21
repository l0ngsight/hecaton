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
package uk.org.hecaton.hd.map;

import uk.org.hecaton.core.EntryView;
import uk.org.hecaton.hd.memory.HdMemoryManager;
import uk.org.hecaton.hd.table.IterationPointers;
import uk.org.hecaton.hd.table.NativeHashTable;
import uk.org.hecaton.hd.table.NativeRecordTable;
import uk.org.hecaton.internal.iteration.IterationPointer;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.map.impl.EntryCostEstimator;
import uk.org.hecaton.map.impl.iterator.MapEntriesWithCursor;
import uk.org.hecaton.map.impl.iterator.MapKeysWithCursor;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.recordstore.LazyEvictableEntryView;
import uk.org.hecaton.map.impl.recordstore.Storage;
import uk.org.hecaton.map.impl.recordstore.expiry.ExpirySystem;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The entries of one partition of a {@code NATIVE} map: keys, values and record metadata are in
 * native memory, in a {@link NativeRecordTable}. Keys and values are handed out as heap copies.
 * <p>
 * Like the other storages, it is used by its partition's thread only.
 */
public final class NativeStorage implements Storage<Data, NativeRecord> {

    static final NativeRecordTable.RecordType<NativeRecord> RECORD_TYPE = new NativeRecordTable.RecordType<>() {
        @Override
        public Class<NativeRecord> recordClass() {
            return NativeRecord.class;
        }

        @Override
        public NativeRecord view(HdMemoryManager memory, long block) {
            return new NativeRecord(memory, block);
        }

        @Override
        public NativeRecord copyOf(Object record) {
            return NativeRecord.copyOf((Record<?>) record);
        }

        @Override
        public void free(HdMemoryManager memory, long block, boolean freeKey) {
            NativeRecord.free(memory, block, freeKey);
        }
    };

    private final NativeRecordTable<NativeRecord> records;
    private final ExpirySystem expirySystem;
    private final SerializationService serializationService;
    private EntryCostEstimator entryCostEstimator = new NativeCostEstimator();

    public NativeStorage(HdMemoryManager memory, ExpirySystem expirySystem, SerializationService serializationService) {
        this.records = new NativeRecordTable<>(memory, RECORD_TYPE);
        this.expirySystem = expirySystem;
        this.serializationService = serializationService;
    }

    @Override
    public void put(Data key, NativeRecord record) {
        records.put(key, record, false);
    }

    @Override
    public NativeRecord updateRecordValue(Data key, NativeRecord record, Object value) {
        record.setValue(serializationService.toData(value));
        return record;
    }

    @Override
    public NativeRecord get(Data key) {
        return records.get(key);
    }

    @Override
    public NativeRecord getIfSameKey(Data key) {
        throw new UnsupportedOperationException("NativeStorage#getIfSameKey");
    }

    @Override
    public void removeRecord(Data dataKey, NativeRecord record) {
        records.remove(dataKey, record);
    }

    @Override
    public boolean containsKey(Data key) {
        return records.containsKey(key);
    }

    @Override
    public Iterator<Map.Entry<Data, NativeRecord>> mutationTolerantIterator() {
        NativeHashTable.Cursor cursor = new NativeHashTable.Cursor();
        return new Iterator<>() {
            private long next = -1;

            @Override
            public boolean hasNext() {
                if (next == -1) {
                    next = records.next(cursor);
                }
                return next != 0;
            }

            @Override
            public Map.Entry<Data, NativeRecord> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                long block = next;
                next = -1;
                return new AbstractMap.SimpleImmutableEntry<>(records.keyOf(block), records.view(block));
            }
        };
    }

    @Override
    public int size() {
        return records.size();
    }

    @Override
    public boolean isEmpty() {
        return records.size() == 0;
    }

    @Override
    public void clear() {
        records.clear();
    }

    @Override
    public void destroy() {
        records.destroy();
    }

    @Override
    public EntryCostEstimator getEntryCostEstimator() {
        return entryCostEstimator;
    }

    @Override
    public void setEntryCostEstimator(EntryCostEstimator entryCostEstimator) {
        this.entryCostEstimator = entryCostEstimator;
    }

    @Override
    public Iterable<EntryView> getRandomSamples(int sampleCount) {
        long[] blocks = new long[sampleCount];
        int found = records.sample(ThreadLocalRandom.current().nextInt(), blocks, sampleCount);
        List<EntryView> samples = new ArrayList<>(found);
        for (int i = 0; i < found; i++) {
            Data key = records.keyOf(blocks[i]);
            samples.add(new LazyEvictableEntryView<>(key, records.view(blocks[i]), expirySystem.getExpiryMetadata(key),
                    serializationService));
        }
        return samples;
    }

    @Override
    public MapKeysWithCursor fetchKeys(IterationPointer[] pointers, int size) {
        NativeHashTable.Cursor cursor = IterationPointers.toCursor(pointers);
        List<Data> keys = new ArrayList<>(size);
        long block = 0;
        while (keys.size() < size && (block = records.next(cursor)) != 0) {
            keys.add(records.keyOf(block));
        }
        return new MapKeysWithCursor(keys, IterationPointers.fromCursor(cursor, block == 0));
    }

    @Override
    public MapEntriesWithCursor fetchEntries(IterationPointer[] pointers, int size) {
        NativeHashTable.Cursor cursor = IterationPointers.toCursor(pointers);
        List<Map.Entry<Data, Data>> entries = new ArrayList<>(size);
        long block = 0;
        while (entries.size() < size && (block = records.next(cursor)) != 0) {
            entries.add(new AbstractMap.SimpleEntry<>(records.keyOf(block), records.view(block).getValue()));
        }
        return new MapEntriesWithCursor(entries, IterationPointers.fromCursor(cursor, block == 0));
    }

    @Override
    public Data extractDataKeyFromLazy(EntryView entryView) {
        return ((LazyEvictableEntryView) entryView).getDataKey();
    }

    @Override
    public Data toBackingDataKeyFormat(Data key) {
        return key;
    }

    /** Native bytes of the entries and the table. */
    public long footprint() {
        return records.footprint();
    }

    /** Reports the native bytes of the entries as the owned entry cost. */
    private final class NativeCostEstimator implements EntryCostEstimator<Data, Record> {

        @Override
        public long getEstimate() {
            return records.recordFootprint();
        }

        @Override
        public void adjustEstimateBy(long adjustment) {
            // the record table keeps its own count
        }

        @Override
        public long calculateValueCost(Record record) {
            return record.getCost();
        }

        @Override
        public long calculateEntryCost(Data key, Record record) {
            return record.getCost();
        }

        @Override
        public void reset() {
            // the record table keeps its own count
        }
    }
}
