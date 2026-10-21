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

import uk.org.hecaton.cache.impl.CacheDataSerializerHook;
import uk.org.hecaton.cache.impl.record.CacheRecord;
import uk.org.hecaton.hd.memory.HdMemoryManager;
import uk.org.hecaton.hd.memory.NativeAccess;
import uk.org.hecaton.hd.memory.NativeData;
import uk.org.hecaton.hd.table.NativeBlockRecord;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * A cache record in a native block (see {@link NativeBlockRecord}). It serializes as a
 * {@link uk.org.hecaton.cache.impl.record.CacheDataRecord}, so replicas receive heap records.
 * <p>
 * Record block layout: key, value and expiry policy block addresses, creation, expiration and last
 * access times, hits.
 */
public final class NativeCacheRecord implements CacheRecord<Data, Data>, NativeBlockRecord, IdentifiedDataSerializable {

    static final long KEY = 0;
    static final long VALUE = 8;
    static final long EXPIRY_POLICY = 16;
    static final long CREATION = 24;
    static final long EXPIRATION = 32;
    static final long ACCESS = 40;
    static final long HITS = 48;
    static final long SIZE = 56;

    private HdMemoryManager memory;
    private long address;
    private Data cachedValue;

    private Data value;
    private Data expiryPolicy;
    private long creationTime = TIME_NOT_AVAILABLE;
    private long expirationTime = TIME_NOT_AVAILABLE;
    private long lastAccessTime = TIME_NOT_AVAILABLE;
    private int hits;

    NativeCacheRecord(Data value, long creationTime, long expirationTime) {
        this.value = value;
        this.creationTime = creationTime;
        this.expirationTime = expirationTime;
    }

    NativeCacheRecord(HdMemoryManager memory, long address) {
        this.memory = memory;
        this.address = address;
    }

    /** A detached copy of any cache record. */
    static NativeCacheRecord copyOf(CacheRecord<?, ?> record) {
        NativeCacheRecord copy = new NativeCacheRecord((Data) record.getValue(), record.getCreationTime(),
                record.getExpirationTime());
        copy.expiryPolicy = (Data) record.getExpiryPolicy();
        copy.lastAccessTime = record.getLastAccessTime();
        copy.hits = (int) Math.min(Integer.MAX_VALUE, record.getHits());
        return copy;
    }

    static void free(HdMemoryManager memory, long block, boolean freeKey) {
        freeData(memory, NativeAccess.getLong(block + VALUE));
        freeData(memory, NativeAccess.getLong(block + EXPIRY_POLICY));
        if (freeKey) {
            NativeData.free(memory, NativeAccess.getLong(block + KEY));
        }
        memory.free(block, SIZE);
    }

    static long footprint(long block) {
        return SIZE + NativeData.blockSize(NativeAccess.getLong(block + KEY))
                + dataSize(NativeAccess.getLong(block + VALUE)) + dataSize(NativeAccess.getLong(block + EXPIRY_POLICY));
    }

    private static long dataSize(long block) {
        return block == 0 ? 0 : NativeData.blockSize(block);
    }

    private static void freeData(HdMemoryManager memory, long block) {
        if (block != 0) {
            NativeData.free(memory, block);
        }
    }

    @Override
    public long address() {
        return address;
    }

    @Override
    public long attach(HdMemoryManager memory, long keyBlock) {
        long valueBlock = 0;
        long policyBlock = 0;
        try {
            valueBlock = value == null ? 0 : NativeData.write(memory, value);
            policyBlock = expiryPolicy == null ? 0 : NativeData.write(memory, expiryPolicy);
            long block = memory.allocate(SIZE);
            NativeAccess.putLong(block + KEY, keyBlock);
            NativeAccess.putLong(block + VALUE, valueBlock);
            NativeAccess.putLong(block + EXPIRY_POLICY, policyBlock);
            NativeAccess.putLong(block + CREATION, creationTime);
            NativeAccess.putLong(block + EXPIRATION, expirationTime);
            NativeAccess.putLong(block + ACCESS, lastAccessTime);
            NativeAccess.putInt(block + HITS, hits);
            this.memory = memory;
            this.address = block;
            this.cachedValue = value;
            this.value = null;
            this.expiryPolicy = null;
            return block;
        } catch (RuntimeException | Error e) {
            freeData(memory, valueBlock);
            freeData(memory, policyBlock);
            throw e;
        }
    }

    @Override
    public void detach() {
        if (address == 0) {
            return;
        }
        value = getValue();
        expiryPolicy = getExpiryPolicy();
        creationTime = NativeAccess.getLong(address + CREATION);
        expirationTime = NativeAccess.getLong(address + EXPIRATION);
        lastAccessTime = NativeAccess.getLong(address + ACCESS);
        hits = NativeAccess.getInt(address + HITS);
        address = 0;
        cachedValue = null;
    }

    @Override
    public Data getValue() {
        if (address == 0) {
            return value;
        }
        Data heapValue = cachedValue;
        if (heapValue == null) {
            long block = NativeAccess.getLong(address + VALUE);
            if (block != 0) {
                heapValue = NativeData.read(block);
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
        replaceData(VALUE, newValue);
        cachedValue = newValue;
    }

    @Override
    public void setExpiryPolicy(Data policy) {
        if (address == 0) {
            expiryPolicy = policy;
        } else {
            replaceData(EXPIRY_POLICY, policy);
        }
    }

    @Override
    public Data getExpiryPolicy() {
        if (address == 0) {
            return expiryPolicy;
        }
        long block = NativeAccess.getLong(address + EXPIRY_POLICY);
        return block == 0 ? null : NativeData.read(block);
    }

    private void replaceData(long field, Data data) {
        long newBlock = data == null ? 0 : NativeData.write(memory, data);
        long oldBlock = NativeAccess.getLong(address + field);
        NativeAccess.putLong(address + field, newBlock);
        freeData(memory, oldBlock);
    }

    @Override
    public long getCreationTime() {
        return address == 0 ? creationTime : NativeAccess.getLong(address + CREATION);
    }

    @Override
    public void setCreationTime(long time) {
        if (address == 0) {
            creationTime = time;
        } else {
            NativeAccess.putLong(address + CREATION, time);
        }
    }

    @Override
    public long getExpirationTime() {
        return address == 0 ? expirationTime : NativeAccess.getLong(address + EXPIRATION);
    }

    @Override
    public void setExpirationTime(long time) {
        if (address == 0) {
            expirationTime = time;
        } else {
            NativeAccess.putLong(address + EXPIRATION, time);
        }
    }

    @Override
    public boolean isExpiredAt(long now) {
        long expiration = getExpirationTime();
        return expiration > TIME_NOT_AVAILABLE && expiration <= now;
    }

    @Override
    public long getLastAccessTime() {
        return address == 0 ? lastAccessTime : NativeAccess.getLong(address + ACCESS);
    }

    @Override
    public void setLastAccessTime(long time) {
        if (address == 0) {
            lastAccessTime = time;
        } else {
            NativeAccess.putLong(address + ACCESS, time);
        }
    }

    @Override
    public long getHits() {
        return address == 0 ? hits : NativeAccess.getInt(address + HITS);
    }

    @Override
    public void setHits(long hit) {
        int value32 = (int) Math.min(Integer.MAX_VALUE, hit);
        if (address == 0) {
            hits = value32;
        } else {
            NativeAccess.putInt(address + HITS, value32);
        }
    }

    @Override
    public void incrementHits() {
        long current = getHits();
        if (current < Integer.MAX_VALUE) {
            setHits(current + 1);
        }
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.CACHE_DATA_RECORD;
    }

    /** Written as a {@link uk.org.hecaton.cache.impl.record.CacheDataRecord}. */
    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(getCreationTime());
        out.writeLong(getExpirationTime());
        out.writeLong(getLastAccessTime());
        out.writeInt((int) getHits());
        IOUtil.writeData(out, getValue());
        IOUtil.writeData(out, getExpiryPolicy());
    }

    @Override
    public void readData(ObjectDataInput in) {
        throw new UnsupportedOperationException("Native cache records are read as CacheDataRecords");
    }

    @Override
    public String toString() {
        return "NativeCacheRecord{" + (address == 0 ? "detached" : "address=" + address) + '}';
    }
}
