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
package uk.org.hecaton.hd.cache;

import uk.org.hecaton.cache.CacheEntryView;
import uk.org.hecaton.cache.impl.CacheContext;
import uk.org.hecaton.cache.impl.CacheEntriesWithCursor;
import uk.org.hecaton.cache.impl.CacheKeysWithCursor;
import uk.org.hecaton.cache.impl.record.CacheRecord;
import uk.org.hecaton.cache.impl.record.SampleableCacheRecordMap;
import uk.org.hecaton.hd.memory.HdMemoryManager;
import uk.org.hecaton.hd.table.IterationPointers;
import uk.org.hecaton.hd.table.NativeHashTable;
import uk.org.hecaton.hd.table.NativeRecordTable;
import uk.org.hecaton.internal.eviction.EvictionCandidate;
import uk.org.hecaton.internal.eviction.EvictionListener;
import uk.org.hecaton.internal.iteration.IterationPointer;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

/**
 * The records of one partition of a {@code NATIVE} cache, in native memory (a
 * {@link NativeRecordTable}). Used by its partition's thread only.
 */
public final class NativeCacheRecordMap extends AbstractMap<Data, CacheRecord>
        implements SampleableCacheRecordMap<Data, CacheRecord> {

    static final NativeRecordTable.RecordType<NativeCacheRecord> RECORD_TYPE = new NativeRecordTable.RecordType<>() {
        @Override
        public Class<NativeCacheRecord> recordClass() {
            return NativeCacheRecord.class;
        }

        @Override
        public NativeCacheRecord view(HdMemoryManager memory, long block) {
            return new NativeCacheRecord(memory, block);
        }

        @Override
        public NativeCacheRecord copyOf(Object record) {
            return NativeCacheRecord.copyOf((CacheRecord<?, ?>) record);
        }

        @Override
        public void free(HdMemoryManager memory, long block, boolean freeKey) {
            NativeCacheRecord.free(memory, block, freeKey);
        }
    };

    private final NativeRecordTable<NativeCacheRecord> records;
    private final SerializationService serializationService;
    private final CacheContext cacheContext;
    private boolean entryCounting;

    /**
     * @param cacheFootprint counts the native bytes of the records of all partitions of the cache
     */
    public NativeCacheRecordMap(HdMemoryManager memory, SerializationService serializationService,
                                CacheContext cacheContext, LongAdder cacheFootprint) {
        this.records = new NativeRecordTable<>(memory, RECORD_TYPE, cacheFootprint);
        this.serializationService = serializationService;
        this.cacheContext = cacheContext;
    }

    @Override
    public void setEntryCounting(boolean enable) {
        if (enable && !entryCounting) {
            cacheContext.increaseEntryCount(size());
        } else if (!enable && entryCounting) {
            cacheContext.decreaseEntryCount(size());
        }
        entryCounting = enable;
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
    public boolean containsKey(Object key) {
        return key instanceof Data data && records.containsKey(data);
    }

    @Override
    public CacheRecord get(Object key) {
        return key instanceof Data data ? records.get(data) : null;
    }

    @Override
    public CacheRecord put(Data key, CacheRecord record) {
        boolean existed = records.containsKey(key);
        CacheRecord previous = records.put(key, record, true);
        if (!existed && entryCounting) {
            cacheContext.increaseEntryCount();
        }
        return previous;
    }

    @Override
    public CacheRecord putIfAbsent(Data key, CacheRecord record) {
        CacheRecord current = records.get(key);
        if (current != null) {
            return current;
        }
        put(key, record);
        return null;
    }

    @Override
    public CacheRecord remove(Object key) {
        if (!(key instanceof Data data)) {
            return null;
        }
        CacheRecord removed = records.remove(data, null);
        if (removed != null && entryCounting) {
            cacheContext.decreaseEntryCount();
        }
        return removed;
    }

    @Override
    public boolean remove(Object key, Object value) {
        if (!(key instanceof Data data)) {
            return false;
        }
        CacheRecord current = records.get(data);
        if (current == null || current != value) {
            return false;
        }
        remove(data);
        return true;
    }

    @Override
    public void clear() {
        int size = size();
        records.clear();
        if (entryCounting) {
            cacheContext.decreaseEntryCount(size);
        }
    }

    /** Frees the records and the table; the map cannot be used afterwards. */
    public void destroy() {
        clear();
        records.destroy();
    }

    /** Native bytes of the records and the table. */
    public long footprint() {
        return records.footprint();
    }

    @Override
    public Set<Entry<Data, CacheRecord>> entrySet() {
        return new EntrySet();
    }

    @Override
    public CacheKeysWithCursor fetchKeys(IterationPointer[] pointers, int size) {
        NativeHashTable.Cursor cursor = IterationPointers.toCursor(pointers);
        List<Data> keys = new ArrayList<>(size);
        long block = 0;
        while (keys.size() < size && (block = records.next(cursor)) != 0) {
            keys.add(records.keyOf(block));
        }
        return new CacheKeysWithCursor(keys, IterationPointers.fromCursor(cursor, block == 0));
    }

    @Override
    public CacheEntriesWithCursor fetchEntries(IterationPointer[] pointers, int size) {
        NativeHashTable.Cursor cursor = IterationPointers.toCursor(pointers);
        List<Map.Entry<Data, Data>> entries = new ArrayList<>(size);
        long block = 0;
        while (entries.size() < size && (block = records.next(cursor)) != 0) {
            entries.add(new SimpleEntry<>(records.keyOf(block), records.view(block).getValue()));
        }
        return new CacheEntriesWithCursor(entries, IterationPointers.fromCursor(cursor, block == 0));
    }

    @Override
    public <C extends EvictionCandidate<Data, CacheRecord>> boolean tryEvict(C evictionCandidate,
                                                                            EvictionListener<Data, CacheRecord> listener) {
        if (evictionCandidate == null) {
            return false;
        }
        CacheRecord removed = remove(evictionCandidate.getAccessor());
        if (removed == null) {
            return false;
        }
        if (listener != null) {
            listener.onEvict(evictionCandidate.getAccessor(), removed, false);
        }
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterable<SamplingEntry> sample(int sampleCount) {
        long[] blocks = new long[sampleCount];
        int found = records.sample(ThreadLocalRandom.current().nextInt(), blocks, sampleCount);
        List<SamplingEntry> samples = new ArrayList<>(found);
        for (int i = 0; i < found; i++) {
            samples.add(new SamplingEntry(records.keyOf(blocks[i]), records.view(blocks[i])));
        }
        return samples;
    }

    /** A sampled record: an eviction candidate and the entry view an eviction policy sees. */
    @SuppressWarnings("rawtypes")
    final class SamplingEntry implements EvictionCandidate, CacheEntryView {

        private final Data key;
        private final CacheRecord record;

        SamplingEntry(Data key, CacheRecord record) {
            this.key = key;
            this.record = record;
        }

        @Override
        public Data getAccessor() {
            return key;
        }

        @Override
        public CacheRecord getEvictable() {
            return record;
        }

        @Override
        public Object getKey() {
            return serializationService.toObject(key);
        }

        @Override
        public Object getValue() {
            return serializationService.toObject(record.getValue());
        }

        @Override
        public Object getExpiryPolicy() {
            return serializationService.toObject(record.getExpiryPolicy());
        }

        @Override
        public long getCreationTime() {
            return record.getCreationTime();
        }

        @Override
        public long getExpirationTime() {
            return record.getExpirationTime();
        }

        @Override
        public long getLastAccessTime() {
            return record.getLastAccessTime();
        }

        @Override
        public long getHits() {
            return record.getHits();
        }
    }

    private final class EntrySet extends AbstractSet<Entry<Data, CacheRecord>> {

        @Override
        public Iterator<Entry<Data, CacheRecord>> iterator() {
            return new EntryIterator();
        }

        @Override
        public int size() {
            return records.size();
        }
    }

    /** Iterates by hash cursor, so it tolerates changes to the map. */
    private final class EntryIterator implements Iterator<Entry<Data, CacheRecord>> {

        private final NativeHashTable.Cursor cursor = new NativeHashTable.Cursor();
        private long next = -1;
        private Data lastKey;

        @Override
        public boolean hasNext() {
            if (next == -1) {
                next = records.next(cursor);
            }
            return next != 0;
        }

        @Override
        public Entry<Data, CacheRecord> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            long block = next;
            next = -1;
            lastKey = records.keyOf(block);
            return new SimpleImmutableEntry<>(lastKey, records.view(block));
        }

        @Override
        public void remove() {
            if (lastKey == null) {
                throw new IllegalStateException();
            }
            NativeCacheRecordMap.this.remove(lastKey);
            lastKey = null;
        }
    }
}
