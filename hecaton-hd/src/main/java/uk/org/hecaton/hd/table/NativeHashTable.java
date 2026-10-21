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

import uk.org.hecaton.hd.memory.HdMemoryManager;
import uk.org.hecaton.hd.memory.NativeAccess;
import uk.org.hecaton.hd.memory.NativeData;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.util.HashUtil;

import java.util.function.LongConsumer;

/**
 * An open-addressing hash table in native memory from serialized keys to entry blocks. An entry
 * block is owned by the caller and starts with the address of its key block (a
 * {@link NativeData} block).
 * <p>
 * The table is a sorted array: slots hold the entries in increasing order of their (unsigned,
 * mixed) key hash, and an entry is never before its home slot, the one given by the high bits of
 * its hash. Lookups scan from the home slot until they pass the hash; inserting shifts the run
 * after the new entry right by one slot, removing shifts it back. Because the order depends only
 * on the hashes, an iteration position is a hash and the number of entries with that hash already
 * seen ({@link Cursor}): it stays valid when the table is resized or changed.
 * <p>
 * Not thread-safe; {@link #size()} may be read by any thread.
 */
public final class NativeHashTable {

    private static final int SLOT_SIZE = 16;
    private static final int HASH = 0;
    private static final int ENTRY = 8;
    private static final int MIN_CAPACITY = 16;
    private static final int MIN_OVERFLOW = 64;
    private static final int OVERFLOW_SHIFT = 4;
    /** Grow when more than 5/8 of the home slots are used. */
    private static final int LOAD_NUMERATOR = 5;
    private static final int LOAD_SHIFT = 3;

    private final HdMemoryManager memory;
    private long slots;
    private int capacity;
    private int slotCount;
    private int shift;
    private volatile int size;

    public NativeHashTable(HdMemoryManager memory) {
        this.memory = memory;
        allocate(MIN_CAPACITY);
    }

    /** Spreads a key's {@link Data#hashCode()} with the murmur3 finalizer. */
    public static int hashOf(Data key) {
        return HashUtil.MurmurHash3_fmix(key.hashCode());
    }

    public int size() {
        return size;
    }

    /** @return the entry for {@code key}, or 0 */
    public long get(int hash, Data key) {
        int index = indexOf(hash, key);
        return index < 0 ? 0 : entryAt(index);
    }

    /**
     * Adds an entry for a key that is not in the table.
     *
     * @throws uk.org.hecaton.memory.NativeOutOfMemoryError if the table cannot grow; it is unchanged then
     */
    public void insert(int hash, long entry) {
        if (size + 1 > ((long) capacity * LOAD_NUMERATOR) >>> LOAD_SHIFT) {
            resize(capacity << 1);
        }
        while (true) {
            int index = home(hash);
            while (index < slotCount && entryAt(index) != 0 && Integer.compareUnsigned(hashAt(index), hash) <= 0) {
                index++;
            }
            int empty = index;
            while (empty < slotCount && entryAt(empty) != 0) {
                empty++;
            }
            if (empty == slotCount) {
                // the run reached the end of the overflow slots
                resize(capacity << 1);
                continue;
            }
            if (empty > index) {
                NativeAccess.copy(slot(index), slot(index + 1), (long) (empty - index) * SLOT_SIZE);
            }
            NativeAccess.putInt(slot(index) + HASH, hash);
            NativeAccess.putLong(slot(index) + ENTRY, entry);
            size++;
            return;
        }
    }

    /**
     * Replaces the entry of a key in the table.
     *
     * @return the previous entry
     */
    public long replace(int hash, Data key, long entry) {
        int index = indexOf(hash, key);
        if (index < 0) {
            throw new IllegalArgumentException("Key is not in the table");
        }
        long previous = entryAt(index);
        NativeAccess.putLong(slot(index) + ENTRY, entry);
        return previous;
    }

    /** @return the removed entry, or 0 */
    public long remove(int hash, Data key) {
        int index = indexOf(hash, key);
        if (index < 0) {
            return 0;
        }
        long entry = entryAt(index);
        // shift the displaced entries after it back by one slot
        int end = index + 1;
        while (end < slotCount && entryAt(end) != 0 && home(hashAt(end)) < end) {
            end++;
        }
        if (end > index + 1) {
            NativeAccess.copy(slot(index + 1), slot(index), (long) (end - index - 1) * SLOT_SIZE);
        }
        NativeAccess.putLong(slot(end - 1) + ENTRY, 0);
        size--;
        if (capacity > MIN_CAPACITY && size < capacity >>> LOAD_SHIFT) {
            try {
                resize(capacity >>> 1);
            } catch (RuntimeException | Error e) {
                // shrinking is optional; keep the larger table if a smaller one cannot be allocated
            }
        }
        return entry;
    }

    /**
     * The entry at or after {@code cursor}'s position, advancing the cursor past it.
     *
     * @return the entry, or 0 at the end
     */
    public long next(Cursor cursor) {
        int index = cursor.started ? seek(cursor.hash, cursor.skip) : firstEntry(0);
        if (index < 0) {
            return 0;
        }
        int hash = hashAt(index);
        if (cursor.started && hash == cursor.hash) {
            cursor.skip++;
        } else {
            cursor.hash = hash;
            cursor.skip = 1;
            cursor.started = true;
        }
        return entryAt(index);
    }

    /**
     * Collects up to {@code count} entries starting at a random position.
     *
     * @param start any int; the position is derived from it
     */
    public int sample(int start, long[] entries, int count) {
        int found = 0;
        int index = Math.floorMod(start, slotCount);
        for (int visited = 0; visited < slotCount && found < count; visited++) {
            long entry = entryAt(index);
            if (entry != 0) {
                entries[found++] = entry;
            }
            index = index + 1 == slotCount ? 0 : index + 1;
        }
        return found;
    }

    /** Calls {@code action} for every entry, then empties the table. */
    public void clear(LongConsumer action) {
        for (int i = 0; i < slotCount; i++) {
            long entry = entryAt(i);
            if (entry != 0) {
                action.accept(entry);
            }
        }
        NativeAccess.fill(slots, (long) slotCount * SLOT_SIZE, (byte) 0);
        size = 0;
    }

    /** Frees the table; its entries must have been cleared. */
    public void dispose() {
        if (slots != 0) {
            memory.freeMetadata(slots, (long) slotCount * SLOT_SIZE);
            slots = 0;
            slotCount = 0;
        }
    }

    /** Native bytes of the table itself. */
    public long footprint() {
        return (long) slotCount * SLOT_SIZE;
    }

    /** The key block of an entry. */
    public static long keyOf(long entry) {
        return NativeAccess.getLong(entry);
    }

    private int indexOf(int hash, Data key) {
        for (int index = home(hash); index < slotCount; index++) {
            long entry = entryAt(index);
            if (entry == 0) {
                return -1;
            }
            int compared = Integer.compareUnsigned(hashAt(index), hash);
            if (compared > 0) {
                return -1;
            }
            if (compared == 0 && NativeData.equals(keyOf(entry), key)) {
                return index;
            }
        }
        return -1;
    }

    /** The first entry with hash above {@code hash}, or the one after {@code skip} entries with that hash. */
    private int seek(int hash, int skip) {
        int remaining = skip;
        for (int index = home(hash); index < slotCount; index++) {
            if (entryAt(index) == 0) {
                continue;
            }
            int compared = Integer.compareUnsigned(hashAt(index), hash);
            if (compared > 0) {
                return index;
            }
            if (compared == 0) {
                if (remaining == 0) {
                    return index;
                }
                remaining--;
            }
        }
        return -1;
    }

    private int firstEntry(int from) {
        for (int index = from; index < slotCount; index++) {
            if (entryAt(index) != 0) {
                return index;
            }
        }
        return -1;
    }

    private void allocate(int newCapacity) {
        int newSlotCount = newCapacity + Math.max(MIN_OVERFLOW, newCapacity >>> OVERFLOW_SHIFT);
        long bytes = (long) newSlotCount * SLOT_SIZE;
        long newSlots = memory.allocateMetadata(bytes);
        NativeAccess.fill(newSlots, bytes, (byte) 0);
        slots = newSlots;
        capacity = newCapacity;
        slotCount = newSlotCount;
        shift = Integer.SIZE - Integer.numberOfTrailingZeros(newCapacity);
    }

    private void resize(int newCapacity) {
        long oldSlots = slots;
        int oldSlotCount = slotCount;
        int oldCapacity = capacity;
        int oldShift = shift;
        int target = Math.max(MIN_CAPACITY, newCapacity);
        while (true) {
            allocate(target);
            // entries are sorted by hash, so their new home slots are non-decreasing
            int next = 0;
            boolean fits = true;
            for (int i = 0; i < oldSlotCount && fits; i++) {
                long entry = NativeAccess.getLong(oldSlots + (long) i * SLOT_SIZE + ENTRY);
                if (entry == 0) {
                    continue;
                }
                int hash = NativeAccess.getInt(oldSlots + (long) i * SLOT_SIZE + HASH);
                int index = Math.max(home(hash), next);
                if (index >= slotCount) {
                    fits = false;
                } else {
                    NativeAccess.putInt(slot(index) + HASH, hash);
                    NativeAccess.putLong(slot(index) + ENTRY, entry);
                    next = index + 1;
                }
            }
            if (fits) {
                memory.freeMetadata(oldSlots, (long) oldSlotCount * SLOT_SIZE);
                return;
            }
            // too many entries at the end of a smaller table: try a larger one
            memory.freeMetadata(slots, (long) slotCount * SLOT_SIZE);
            slots = oldSlots;
            slotCount = oldSlotCount;
            capacity = oldCapacity;
            shift = oldShift;
            target <<= 1;
        }
    }

    private int home(int hash) {
        return (int) (Integer.toUnsignedLong(hash) >>> shift);
    }

    private long slot(int index) {
        return slots + (long) index * SLOT_SIZE;
    }

    private long entryAt(int index) {
        return NativeAccess.getLong(slot(index) + ENTRY);
    }

    private int hashAt(int index) {
        return NativeAccess.getInt(slot(index) + HASH);
    }

    /**
     * A position in a {@link NativeHashTable}: the hash of the last entry returned and how many
     * entries with that hash were returned.
     */
    public static final class Cursor {
        private boolean started;
        private int hash;
        private int skip;

        public Cursor() {
        }

        public Cursor(int hash, int skip) {
            this.started = true;
            this.hash = hash;
            this.skip = skip;
        }

        public boolean started() {
            return started;
        }

        public int hash() {
            return hash;
        }

        public int skip() {
            return skip;
        }
    }
}
