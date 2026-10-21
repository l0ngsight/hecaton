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

import uk.org.hecaton.hd.memory.HdMemoryManager;
import uk.org.hecaton.hd.memory.NativeAccess;
import uk.org.hecaton.hd.memory.NativeData;
import uk.org.hecaton.hd.table.NativeBlockRecord;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.record.RecordReaderWriter;

import static uk.org.hecaton.internal.util.TimeStripUtil.recomputeWithBaseTime;
import static uk.org.hecaton.internal.util.TimeStripUtil.stripBaseTime;

/**
 * A map record whose state is either in a native block ({@link #isAttached() attached}, while it
 * is in a {@link NativeStorage}) or in this object (detached: created by the record factory and not
 * stored yet, or removed from the storage). Values are handed out as heap copies, so nothing a
 * caller keeps refers to native memory.
 * <p>
 * Record block layout: key block address, value block address, version, creation, last access,
 * last update and last stored times (as {@link uk.org.hecaton.internal.util.TimeStripUtil} seconds),
 * hits.
 */
public final class NativeRecord implements Record<Data>, NativeBlockRecord {

    static final long KEY = 0;
    static final long VALUE = 8;
    static final long VERSION = 16;
    static final long CREATION = 20;
    static final long ACCESS = 24;
    static final long UPDATE = 28;
    static final long STORED = 32;
    static final long HITS = 36;
    static final long SIZE = 40;

    private HdMemoryManager memory;
    private long address;
    /** Heap copy of the value, while attached. */
    private Data cachedValue;

    // the state of a detached record
    private Data value;
    private int version;
    private int creationTime = UNSET;
    private int lastAccessTime = UNSET;
    private int lastUpdateTime = UNSET;
    private int lastStoredTime = UNSET;
    private int hits;

    NativeRecord(Data value) {
        this.value = value;
    }

    /** A view of the record block at {@code address}. */
    NativeRecord(HdMemoryManager memory, long address) {
        this.memory = memory;
        this.address = address;
    }

    public boolean isAttached() {
        return address != 0;
    }

    @Override
    public long address() {
        return address;
    }

    @Override
    public long attach(HdMemoryManager memory, long keyBlock) {
        long valueBlock = value == null ? 0 : NativeData.write(memory, value);
        long block;
        try {
            block = memory.allocate(SIZE);
        } catch (RuntimeException | Error e) {
            if (valueBlock != 0) {
                NativeData.free(memory, valueBlock);
            }
            throw e;
        }
        NativeAccess.putLong(block + KEY, keyBlock);
        NativeAccess.putLong(block + VALUE, valueBlock);
        NativeAccess.putInt(block + VERSION, version);
        NativeAccess.putInt(block + CREATION, creationTime);
        NativeAccess.putInt(block + ACCESS, lastAccessTime);
        NativeAccess.putInt(block + UPDATE, lastUpdateTime);
        NativeAccess.putInt(block + STORED, lastStoredTime);
        NativeAccess.putInt(block + HITS, hits);
        this.memory = memory;
        this.address = block;
        this.cachedValue = value;
        this.value = null;
        return block;
    }

    @Override
    public void detach() {
        if (address == 0) {
            return;
        }
        value = getValue();
        version = NativeAccess.getInt(address + VERSION);
        creationTime = NativeAccess.getInt(address + CREATION);
        lastAccessTime = NativeAccess.getInt(address + ACCESS);
        lastUpdateTime = NativeAccess.getInt(address + UPDATE);
        lastStoredTime = NativeAccess.getInt(address + STORED);
        hits = NativeAccess.getInt(address + HITS);
        address = 0;
        cachedValue = null;
    }

    /** A detached copy of any record. */
    static NativeRecord copyOf(Record<?> record) {
        Object recordValue = record.getValue();
        NativeRecord copy = new NativeRecord((Data) recordValue);
        copy.version = record.getVersion();
        copy.creationTime = record.getRawCreationTime();
        copy.lastAccessTime = record.getRawLastAccessTime();
        copy.lastUpdateTime = record.getRawLastUpdateTime();
        copy.lastStoredTime = record.getRawLastStoredTime();
        copy.hits = record.getHits();
        return copy;
    }

    /** Frees a record block with its value block and, if {@code freeKey}, its key block. */
    static void free(HdMemoryManager memory, long block, boolean freeKey) {
        long valueBlock = NativeAccess.getLong(block + VALUE);
        if (valueBlock != 0) {
            NativeData.free(memory, valueBlock);
        }
        if (freeKey) {
            NativeData.free(memory, NativeAccess.getLong(block + KEY));
        }
        memory.free(block, SIZE);
    }

    /** Native bytes of a record block with its key and value blocks. */
    static long footprint(long block) {
        long valueBlock = NativeAccess.getLong(block + VALUE);
        return SIZE + NativeData.blockSize(NativeAccess.getLong(block + KEY))
                + (valueBlock == 0 ? 0 : NativeData.blockSize(valueBlock));
    }

    @Override
    public Data getValue() {
        if (address == 0) {
            return value;
        }
        Data heapValue = cachedValue;
        if (heapValue == null) {
            long valueBlock = NativeAccess.getLong(address + VALUE);
            if (valueBlock != 0) {
                heapValue = NativeData.read(valueBlock);
                cachedValue = heapValue;
            }
        }
        return heapValue;
    }

    @Override
    public void setValue(Data newValue) {
        if (address == 0) {
            value = newValue;
            return;
        }
        long newBlock = newValue == null ? 0 : NativeData.write(memory, newValue);
        long oldBlock = NativeAccess.getLong(address + VALUE);
        NativeAccess.putLong(address + VALUE, newBlock);
        if (oldBlock != 0) {
            NativeData.free(memory, oldBlock);
        }
        cachedValue = newValue;
    }

    @Override
    public long getCost() {
        if (address == 0) {
            return SIZE + (value == null ? 0 : value.totalSize());
        }
        return footprint(address);
    }

    @Override
    public int getVersion() {
        return address == 0 ? version : NativeAccess.getInt(address + VERSION);
    }

    @Override
    public void setVersion(int version) {
        if (address == 0) {
            this.version = version;
        } else {
            NativeAccess.putInt(address + VERSION, version);
        }
    }

    @Override
    public long getLastAccessTime() {
        return recomputeWithBaseTime(getRawLastAccessTime());
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
        setRawLastAccessTime(stripBaseTime(lastAccessTime));
    }

    @Override
    public long getLastUpdateTime() {
        return recomputeWithBaseTime(getRawLastUpdateTime());
    }

    @Override
    public void setLastUpdateTime(long lastUpdateTime) {
        setRawLastUpdateTime(stripBaseTime(lastUpdateTime));
    }

    @Override
    public long getCreationTime() {
        return recomputeWithBaseTime(getRawCreationTime());
    }

    @Override
    public void setCreationTime(long creationTime) {
        setRawCreationTime(stripBaseTime(creationTime));
    }

    @Override
    public long getLastStoredTime() {
        int raw = getRawLastStoredTime();
        return raw == UNSET ? 0L : recomputeWithBaseTime(raw);
    }

    @Override
    public void setLastStoredTime(long lastStoredTime) {
        setRawLastStoredTime(stripBaseTime(lastStoredTime));
    }

    @Override
    public int getHits() {
        return address == 0 ? hits : NativeAccess.getInt(address + HITS);
    }

    @Override
    public void setHits(int hits) {
        if (address == 0) {
            this.hits = hits;
        } else {
            NativeAccess.putInt(address + HITS, hits);
        }
    }

    @Override
    public RecordReaderWriter getMatchingRecordReaderWriter() {
        return RecordReaderWriter.DATA_RECORD_WITH_STATS_READER_WRITER;
    }

    @Override
    public int getRawCreationTime() {
        return address == 0 ? creationTime : NativeAccess.getInt(address + CREATION);
    }

    @Override
    public void setRawCreationTime(int creationTime) {
        if (address == 0) {
            this.creationTime = creationTime;
        } else {
            NativeAccess.putInt(address + CREATION, creationTime);
        }
    }

    @Override
    public int getRawLastAccessTime() {
        return address == 0 ? lastAccessTime : NativeAccess.getInt(address + ACCESS);
    }

    @Override
    public void setRawLastAccessTime(int lastAccessTime) {
        if (address == 0) {
            this.lastAccessTime = lastAccessTime;
        } else {
            NativeAccess.putInt(address + ACCESS, lastAccessTime);
        }
    }

    @Override
    public int getRawLastUpdateTime() {
        return address == 0 ? lastUpdateTime : NativeAccess.getInt(address + UPDATE);
    }

    @Override
    public void setRawLastUpdateTime(int lastUpdateTime) {
        if (address == 0) {
            this.lastUpdateTime = lastUpdateTime;
        } else {
            NativeAccess.putInt(address + UPDATE, lastUpdateTime);
        }
    }

    @Override
    public int getRawLastStoredTime() {
        return address == 0 ? lastStoredTime : NativeAccess.getInt(address + STORED);
    }

    @Override
    public void setRawLastStoredTime(int lastStoredTime) {
        if (address == 0) {
            this.lastStoredTime = lastStoredTime;
        } else {
            NativeAccess.putInt(address + STORED, lastStoredTime);
        }
    }

    @Override
    public String toString() {
        return "NativeRecord{" + (address == 0 ? "detached" : "address=" + address) + ", version=" + getVersion() + '}';
    }
}
