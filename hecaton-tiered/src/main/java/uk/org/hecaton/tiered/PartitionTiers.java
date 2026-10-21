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

package uk.org.hecaton.tiered;

import uk.org.hecaton.hd.memory.HdMemoryManager;
import uk.org.hecaton.hd.memory.NativeData;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.memory.NativeOutOfMemoryError;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * The tiers of one partition of a tiered map. A value is written to the memory tier (native
 * memory); while the map's memory tier holds more than its capacity, the partition moves its least
 * recently used values to its disk tier. A value read from the disk tier on the partition's thread
 * moves back to memory. When the memory tier cannot allocate, the value goes to disk directly.
 * <p>
 * Changed by the partition's thread only. Other threads may read values; like any native value, a
 * value they read must not be moved concurrently (operations on the entries of a partition run on
 * its thread).
 */
final class PartitionTiers {

    /** Below this, garbage in the disk tier is not worth a rewrite. */
    private static final long MIN_GARBAGE_TO_COMPACT = 1 << 20;

    private final HdMemoryManager memory;
    private final MemoryTierBudget budget;
    private final Path directory;
    private final int partitionId;
    /** The values in memory, least recently used first. */
    private final LinkedHashMap<TieredRecord, Boolean> hot = new LinkedHashMap<>(16, 0.75f, true);
    /** The records with their values on disk. */
    private final Set<TieredRecord> cold = Collections.newSetFromMap(new IdentityHashMap<>());
    private DiskTier disk;
    private int generation;
    private volatile Thread partitionThread;
    private volatile long memoryBytes;

    PartitionTiers(HdMemoryManager memory, MemoryTierBudget budget, Path directory, int partitionId) {
        this.memory = memory;
        this.budget = budget;
        this.directory = directory;
        this.partitionId = partitionId;
        this.disk = new DiskTier(file(0));
    }

    static long memoryCost(long address) {
        return NativeData.blockSize(address);
    }

    /** Takes over a record being stored, with its pending value. */
    void attach(TieredRecord record) {
        Data value = record.pending;
        record.pending = null;
        record.tiers = this;
        store(record, value);
    }

    /** Releases the value of a record being removed. */
    void detach(TieredRecord record) {
        release(record);
        record.tiers = null;
        if (cold.isEmpty()) {
            // nothing left on disk
            disk.delete();
        } else {
            compactIfNeeded();
        }
    }

    void store(TieredRecord record, Data value) {
        partitionThread = Thread.currentThread();
        release(record);
        long address;
        try {
            address = NativeData.write(memory, value);
        } catch (NativeOutOfMemoryError e) {
            toDisk(record, value);
            return;
        }
        record.address = address;
        hot.put(record, Boolean.TRUE);
        addMemory(memoryCost(address));
        spill(record);
        compactIfNeeded();
    }

    Data load(TieredRecord record) {
        boolean owner = Thread.currentThread() == partitionThread;
        long address = record.address;
        if (address != 0) {
            if (owner) {
                hot.get(record);
            }
            return NativeData.read(address);
        }
        long offset = record.offset;
        if (offset == TieredRecord.NONE) {
            return null;
        }
        Data value = disk.read(offset, record.length);
        if (owner) {
            store(record, value);
        }
        return value;
    }

    /** Releases all values. */
    void clear() {
        for (TieredRecord record : new ArrayList<>(hot.keySet())) {
            record.tiers = null;
            freeMemory(record);
        }
        hot.clear();
        for (TieredRecord record : cold) {
            record.tiers = null;
            record.offset = TieredRecord.NONE;
        }
        cold.clear();
        disk.delete();
    }

    private void addMemory(long bytes) {
        memoryBytes += bytes;
        budget.add(bytes);
    }

    /** The native memory of the partition's values in the memory tier. */
    long memoryBytes() {
        return memoryBytes;
    }

    int memoryCount() {
        return hot.size();
    }

    int diskCount() {
        return cold.size();
    }

    DiskTier disk() {
        return disk;
    }

    /** Moves least recently used values (other than {@code keep}) to disk while the memory tier is over capacity. */
    private void spill(TieredRecord keep) {
        while (budget.isOver()) {
            TieredRecord eldest = null;
            for (TieredRecord record : hot.keySet()) {
                if (record != keep) {
                    eldest = record;
                    break;
                }
            }
            if (eldest == null) {
                return;
            }
            Data value = NativeData.read(eldest.address);
            hot.remove(eldest);
            toDisk(eldest, value);
            freeMemory(eldest);
        }
    }

    private void toDisk(TieredRecord record, Data value) {
        record.length = DiskTier.length(value);
        record.offset = disk.append(value);
        cold.add(record);
    }

    private void release(TieredRecord record) {
        if (record.address != 0) {
            hot.remove(record);
            freeMemory(record);
        }
        if (record.offset != TieredRecord.NONE) {
            cold.remove(record);
            disk.free(record.length);
            record.offset = TieredRecord.NONE;
        }
    }

    private void freeMemory(TieredRecord record) {
        long address = record.address;
        record.address = 0;
        addMemory(-memoryCost(address));
        NativeData.free(memory, address);
    }

    /** Rewrites the disk tier without its garbage when there is more garbage than live data. */
    private void compactIfNeeded() {
        if (disk.garbage() < MIN_GARBAGE_TO_COMPACT || disk.garbage() <= disk.live()) {
            return;
        }
        DiskTier old = disk;
        DiskTier compacted = new DiskTier(file(++generation));
        for (TieredRecord record : cold) {
            record.offset = compacted.append(old.read(record.offset, record.length));
        }
        disk = compacted;
        old.delete();
    }

    private Path file(int gen) {
        return directory.resolve(partitionId + "-" + gen + ".tier");
    }
}
