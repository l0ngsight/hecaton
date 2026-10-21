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
package uk.org.hecaton.hd.table;

import uk.org.hecaton.hd.memory.CountingMemoryManager;
import uk.org.hecaton.hd.memory.HdMemoryManager;
import uk.org.hecaton.hd.memory.NativeAccess;
import uk.org.hecaton.hd.memory.NativeData;
import uk.org.hecaton.internal.serialization.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import static uk.org.hecaton.hd.table.NativeHashTable.hashOf;

/**
 * Records in native record blocks, indexed by key in a {@link NativeHashTable}; the storage of map
 * and cache partitions.
 * <p>
 * Records are handed out as {@link NativeBlockRecord} views. Recent views are cached, so repeated
 * lookups of a key return the same object; when a record leaves the table, its cached view (and
 * the record object the caller passed in) is detached first, so callers never see freed memory.
 * <p>
 * Used by one thread at a time; {@link #size()} may be read by any thread.
 *
 * @param <R> record type
 */
public final class NativeRecordTable<R extends NativeBlockRecord> {

    private static final int MAX_VIEWS = 4096;

    /** Counts the bytes of the records with their key and data blocks. */
    private final CountingMemoryManager memory;
    private final RecordType<R> type;
    private final NativeHashTable table;
    private final Map<Long, R> views = new HashMap<>();
    private boolean destroyed;

    public NativeRecordTable(HdMemoryManager memory, RecordType<R> type) {
        this(memory, type, null);
    }

    /**
     * @param sharedFootprint also counts the native bytes of the records here, for a total over
     *                        several tables; may be null
     */
    public NativeRecordTable(HdMemoryManager memory, RecordType<R> type, LongAdder sharedFootprint) {
        this.memory = new CountingMemoryManager(memory, sharedFootprint);
        this.type = type;
        this.table = new NativeHashTable(memory);
    }

    public int size() {
        return table.size();
    }

    public R get(Data key) {
        long block = table.get(hashOf(key), key);
        return block == 0 ? null : view(block);
    }

    public boolean containsKey(Data key) {
        return table.get(hashOf(key), key) != 0;
    }

    /**
     * Stores {@code record} for {@code key}. A detached record of type {@code R} becomes a view of
     * the stored block; any other record is copied.
     *
     * @param returnPrevious whether to return the replaced record
     * @return the replaced record, detached, if asked for and there was one, otherwise null
     * @throws uk.org.hecaton.memory.NativeOutOfMemoryError if there is no memory; nothing is changed then
     */
    public R put(Data key, Object record, boolean returnPrevious) {
        int hash = hashOf(key);
        long existing = table.get(hash, key);
        if (existing != 0) {
            if (record instanceof NativeBlockRecord r && r.address() == existing) {
                return null;
            }
            R previous = returnPrevious ? view(existing) : null;
            long block = attach(record, NativeAccess.getLong(existing));
            table.replace(hash, key, block);
            release(existing, false);
            return previous;
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
            release(block, true);
            throw e;
        }
        return null;
    }

    /**
     * Removes the record of {@code key}.
     *
     * @param held the record object the caller holds for the key, if any; it is detached too
     * @return the removed record, detached, or null
     */
    public R remove(Data key, Object held) {
        long block = table.remove(hashOf(key), key);
        if (block == 0) {
            return null;
        }
        R removed = view(block);
        if (held instanceof NativeBlockRecord r && r != removed && r.address() == block) {
            r.detach();
        }
        release(block, true);
        return removed;
    }

    /**
     * The record at or after the cursor's position, advancing the cursor.
     *
     * @return the record block, or 0 at the end
     */
    public long next(NativeHashTable.Cursor cursor) {
        return destroyed ? 0 : table.next(cursor);
    }

    /** Collects up to {@code count} record blocks from a random position. */
    public int sample(int start, long[] blocks, int count) {
        return table.sample(start, blocks, count);
    }

    /** A heap copy of the key of a record block. */
    public Data keyOf(long block) {
        return NativeData.read(NativeAccess.getLong(block));
    }

    /** The view of a record block. */
    public R view(long block) {
        R view = views.get(block);
        if (view == null) {
            view = type.view(memory, block);
            cacheView(block, view);
        }
        return view;
    }

    /** Native bytes of the records and the table. */
    public long footprint() {
        return memory.allocated() + table.footprint();
    }

    /** Native bytes of the records with their key and data blocks. */
    public long recordFootprint() {
        return memory.allocated();
    }

    /** Frees all records. */
    public void clear() {
        if (destroyed) {
            return;
        }
        table.clear(block -> release(block, true));
        views.clear();
    }

    /** Frees all records and the table; the table cannot be used afterwards. */
    public void destroy() {
        if (destroyed) {
            return;
        }
        clear();
        table.dispose();
        destroyed = true;
    }

    private long attach(Object record, long keyBlock) {
        @SuppressWarnings("unchecked")
        R nativeRecord = type.recordClass().isInstance(record) && ((R) record).address() == 0
                ? (R) record : type.copyOf(record);
        long block = nativeRecord.attach(memory, keyBlock);
        cacheView(block, nativeRecord);
        return block;
    }

    private void cacheView(long block, R view) {
        if (views.size() >= MAX_VIEWS) {
            views.clear();
        }
        views.put(block, view);
    }

    private void release(long block, boolean freeKey) {
        R view = views.remove(block);
        if (view != null) {
            view.detach();
        }
        type.free(memory, block, freeKey);
    }

    /**
     * How a {@link NativeRecordTable} handles its records' blocks.
     *
     * @param <R> record type
     */
    public interface RecordType<R extends NativeBlockRecord> {

        Class<R> recordClass();

        /** A view of the record block. */
        R view(HdMemoryManager memory, long block);

        /** A detached copy of a record of another type, or of an attached one. */
        R copyOf(Object record);

        /** Frees a record block with its data blocks and, if {@code freeKey}, its key block. */
        void free(HdMemoryManager memory, long block, boolean freeKey);
    }
}
