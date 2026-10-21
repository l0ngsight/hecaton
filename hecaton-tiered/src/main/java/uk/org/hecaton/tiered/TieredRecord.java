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

import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.map.impl.record.Record;
import uk.org.hecaton.map.impl.record.RecordReaderWriter;

/**
 * The record of an entry of a tiered map. The record and its metadata are on the heap; the value is
 * in the memory tier (a native block) or in the disk tier (a range of the partition's file),
 * managed by the partition's {@link PartitionTiers}. Before the record is stored, the value is
 * kept on the heap.
 */
final class TieredRecord implements Record<Data> {

    static final long NONE = -1;

    /** The partition's tiers, once the record is stored. */
    PartitionTiers tiers;
    /** The value before the record is stored. */
    Data pending;
    /** The address of the value in native memory, or 0. */
    volatile long address;
    /** The offset of the value in the disk tier, or {@link #NONE}. */
    volatile long offset = NONE;
    /** The length of the value in the disk tier. */
    volatile int length;

    private int version;
    private long creationTime;
    private long lastAccessTime;
    private long lastUpdateTime;
    private long lastStoredTime;
    private long sequence;
    private int hits;

    TieredRecord(Data value) {
        this.pending = value;
    }

    /** A tiered copy of a record of another kind (from a migration or a backup). */
    static TieredRecord copyOf(Record<?> record) {
        TieredRecord copy = new TieredRecord((Data) record.getValue());
        copy.setVersion(record.getVersion());
        copy.setCreationTime(record.getCreationTime());
        copy.setLastAccessTime(record.getLastAccessTime());
        copy.setLastUpdateTime(record.getLastUpdateTime());
        copy.setLastStoredTime(record.getLastStoredTime());
        copy.setSequence(record.getSequence());
        copy.setHits(record.getHits());
        return copy;
    }

    boolean inMemory() {
        return address != 0;
    }

    boolean onDisk() {
        return offset != NONE;
    }

    @Override
    public Data getValue() {
        PartitionTiers owner = tiers;
        return owner == null ? pending : owner.load(this);
    }

    @Override
    public void setValue(Data value) {
        PartitionTiers owner = tiers;
        if (owner == null) {
            pending = value;
        } else {
            owner.store(this, value);
        }
    }

    @Override
    public long getCost() {
        return inMemory() ? PartitionTiers.memoryCost(address) : 0;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    @Override
    public long getLastStoredTime() {
        return lastStoredTime;
    }

    @Override
    public void setLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = lastStoredTime;
    }

    @Override
    public long getSequence() {
        return sequence;
    }

    @Override
    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    @Override
    public int getHits() {
        return hits;
    }

    @Override
    public void setHits(int hits) {
        this.hits = hits;
    }

    @Override
    public RecordReaderWriter getMatchingRecordReaderWriter() {
        return RecordReaderWriter.SIMPLE_DATA_RECORD_READER_WRITER;
    }

    @Override
    public String toString() {
        return "TieredRecord{version=" + version + ", " + (inMemory() ? "memory" : onDisk() ? "disk" : "pending") + '}';
    }
}
