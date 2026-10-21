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
import uk.org.hecaton.hd.memory.NativeAccess;
import uk.org.hecaton.hd.memory.NativeData;
import uk.org.hecaton.hd.table.NativeHashTable;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

import static uk.org.hecaton.hd.table.NativeHashTable.hashOf;

/**
 * The entries of one partition of a {@code NATIVE} map: keys, values and record metadata are in
 * native memory, indexed by a {@link NativeHashTable}. Keys and values are handed out as heap
 * copies.
 * <p>
 * Like the other storages, it is used by its partition's thread only.
 */
public final class NativeStorage implements Storage<Data, NativeRecord> {

    /** Recent record views, so repeated lookups of a key return the same object. */
    private static final int MAX_VIEWS = 4096;

    private final HdMemoryManager memory;
    private final NativeHashTable table;
    private final ExpirySystem expirySystem;
    private final SerializationService serializationService;
    private final Map<Long, NativeRecord> views = new HashMap<>();
    private EntryCostEstimator entryCostEstimator = new NativeCostEstimator();
    private boolean destroyed;

    public NativeStorage(HdMemoryManager memory, ExpirySystem expirySystem, SerializationService serializationService) {
        this.memory = memory;
        this.table = new NativeHashTable(memory);
        this.expirySystem = expirySystem;
        this.serializationService = serializationService;
    }

    @Override
    public void put(Data key, NativeRecord record) {
        int hash = hashOf(key);
        long existing = table.get(hash, key);
        if (existing != 0) {
            if (record.address() == existing) {
                return;
            }
            long block = attach(record, NativeAccess.getLong(existing + NativeRecord.KEY));
            table.replace(hash, key, block);
            adjustCost(NativeRecord.footprint(block) - NativeRecord.footprint(existing));
            release(existing, false);
            return;
        }
        long keyBlock = NativeData.write(memory, key);
        long block;
        try {
            block = attach(record, keyBlock);
        } catch (RuntimeException | Error e) {
            NativeData.free(memory, keyBlock);
            throw e;
        }
        try {
            table.insert(hash, block);
        } catch (RuntimeException | Error e) {
            views.remove(block);
            record.detach();
            NativeRecord.free(memory, block, true);
            throw e;
        }
        adjustCost(NativeRecord.footprint(block));
    }

    /** Writes the record into a new record block for the key block; the record becomes a view of it. */
    private long attach(Record record, long keyBlock) {
        NativeRecord nativeRecord = record instanceof NativeRecord r && !r.isAttached() ? r : NativeRecord.copyOf(record);
        long block = nativeRecord.attach(memory, keyBlock);
        cacheView(block, nativeRecord);
        return block;
    }

    @Override
    public NativeRecord updateRecordValue(Data key, NativeRecord record, Object value) {
        long before = record.getCost();
        record.setValue(serializationService.toData(value));
        adjustCost(record.getCost() - before);
        return record;
    }

    @Override
    public NativeRecord get(Data key) {
        long block = table.get(hashOf(key), key);
        return block == 0 ? null : view(block);
    }

    @Override
    public NativeRecord getIfSameKey(Data key) {
        throw new UnsupportedOperationException("NativeStorage#getIfSameKey");
    }

    @Override
    public void removeRecord(Data dataKey, NativeRecord record) {
        long block = table.remove(hashOf(dataKey), dataKey);
        if (block == 0) {
            return;
        }
        if (record != null && record.address() == block) {
            record.detach();
        }
        adjustCost(-NativeRecord.footprint(block));
        release(block, true);
    }

    @Override
    public boolean containsKey(Data key) {
        return table.get(hashOf(key), key) != 0;
    }

    @Override
    public Iterator<Map.Entry<Data, NativeRecord>> mutationTolerantIterator() {
        NativeHashTable.Cursor cursor = new NativeHashTable.Cursor();
        return new Iterator<>() {
            private long next = -1;

            @Override
            public boolean hasNext() {
                if (next == -1) {
                    next = destroyed ? 0 : table.next(cursor);
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
                return new AbstractMap.SimpleImmutableEntry<>(keyOf(block), view(block));
            }
        };
    }

    @Override
    public int size() {
        return table.size();
    }

    @Override
    public boolean isEmpty() {
        return table.size() == 0;
    }

    @Override
    public void clear() {
        if (destroyed) {
            return;
        }
        table.clear(block -> release(block, true));
        views.clear();
        entryCostEstimator.reset();
    }

    @Override
    public void destroy() {
        if (destroyed) {
            return;
        }
        clear();
        table.dispose();
        destroyed = true;
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
        int found = table.sample(ThreadLocalRandom.current().nextInt(), blocks, sampleCount);
        List<EntryView> samples = new ArrayList<>(found);
        for (int i = 0; i < found; i++) {
            Data key = keyOf(blocks[i]);
            samples.add(new LazyEvictableEntryView<>(key, view(blocks[i]), expirySystem.getExpiryMetadata(key),
                    serializationService));
        }
        return samples;
    }

    @Override
    public MapKeysWithCursor fetchKeys(IterationPointer[] pointers, int size) {
        NativeHashTable.Cursor cursor = IterationPointers.toCursor(pointers);
        List<Data> keys = new ArrayList<>(size);
        long block = 0;
        while (keys.size() < size && (block = table.next(cursor)) != 0) {
            keys.add(keyOf(block));
        }
        return new MapKeysWithCursor(keys, IterationPointers.fromCursor(cursor, block == 0));
    }

    @Override
    public MapEntriesWithCursor fetchEntries(IterationPointer[] pointers, int size) {
        NativeHashTable.Cursor cursor = IterationPointers.toCursor(pointers);
        List<Map.Entry<Data, Data>> entries = new ArrayList<>(size);
        long block = 0;
        while (entries.size() < size && (block = table.next(cursor)) != 0) {
            entries.add(new AbstractMap.SimpleEntry<>(keyOf(block), view(block).getValue()));
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

    /** Native bytes of the entries and the table, for tests and statistics. */
    public long footprint() {
        return entryCostEstimator.getEstimate() + table.footprint();
    }

    private Data keyOf(long block) {
        return NativeData.read(NativeAccess.getLong(block + NativeRecord.KEY));
    }

    private NativeRecord view(long block) {
        NativeRecord view = views.get(block);
        if (view == null) {
            view = new NativeRecord(memory, block);
            cacheView(block, view);
        }
        return view;
    }

    private void cacheView(long block, NativeRecord view) {
        if (views.size() >= MAX_VIEWS) {
            views.clear();
        }
        views.put(block, view);
    }

    /** Frees a record block that is no longer in the table; its cached view becomes a detached copy. */
    private void release(long block, boolean freeKey) {
        NativeRecord view = views.remove(block);
        if (view != null) {
            view.detach();
        }
        NativeRecord.free(memory, block, freeKey);
    }

    private void adjustCost(long bytes) {
        entryCostEstimator.adjustEstimateBy(bytes);
    }

    /** Native bytes of the entries. */
    private static final class NativeCostEstimator implements EntryCostEstimator<Data, Record> {
        private long estimate;

        @Override
        public long getEstimate() {
            return estimate;
        }

        @Override
        public void adjustEstimateBy(long adjustment) {
            estimate += adjustment;
        }

        @Override
        public long calculateValueCost(Record record) {
            return record.getCost();
        }

        @Override
        public long calculateEntryCost(Data key, Record record) {
            return record.getCost() + key.totalSize();
        }

        @Override
        public void reset() {
            estimate = 0;
        }
    }
}
