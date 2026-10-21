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
package uk.org.hecaton.hd.nearcache;

import uk.org.hecaton.hd.memory.HdMemoryManager;
import uk.org.hecaton.hd.memory.NativeData;
import uk.org.hecaton.internal.nearcache.impl.record.AbstractNearCacheRecord;
import uk.org.hecaton.internal.serialization.Data;

import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * A Near Cache record whose value is in native memory.
 * <p>
 * Near Cache records are read by any thread, also after they have been replaced or removed, so a
 * value block lives as long as its record: it is freed when the value is replaced (under the
 * record's lock, which readers take too) or when the record is garbage collected.
 */
public final class NativeValueNearCacheRecord extends AbstractNearCacheRecord<Data> {

    private static final Cleaner CLEANER = Cleaner.create();

    private final HdMemoryManager memory;
    private final LongAdder footprint;
    private ValueBlock block;
    private Cleaner.Cleanable cleanable;

    NativeValueNearCacheRecord(HdMemoryManager memory, LongAdder footprint, Data value, long creationTime,
                               long expirationTime) {
        super(null, creationTime, expirationTime);
        this.memory = memory;
        this.footprint = footprint;
        replace(value);
    }

    @Override
    public synchronized Data getValue() {
        return block == null ? null : NativeData.read(block.address);
    }

    @Override
    public synchronized void setValue(Data value) {
        replace(value);
    }

    /** Native bytes of the value. */
    synchronized long valueSize() {
        return block == null ? 0 : block.size;
    }

    private void replace(Data value) {
        ValueBlock newBlock = value == null ? null : ValueBlock.write(memory, footprint, value);
        if (cleanable != null) {
            cleanable.clean();
        }
        block = newBlock;
        cleanable = newBlock == null ? null : CLEANER.register(this, newBlock);
    }

    /** A value block; frees itself once. Refers to nothing that refers to the record. */
    private static final class ValueBlock implements Runnable {
        private final HdMemoryManager memory;
        private final LongAdder footprint;
        private final long address;
        private final long size;
        private final AtomicBoolean freed = new AtomicBoolean();

        private ValueBlock(HdMemoryManager memory, LongAdder footprint, long address, long size) {
            this.memory = memory;
            this.footprint = footprint;
            this.address = address;
            this.size = size;
        }

        static ValueBlock write(HdMemoryManager memory, LongAdder footprint, Data value) {
            long address = NativeData.write(memory, value);
            long size = NativeData.blockSize(address);
            footprint.add(size);
            return new ValueBlock(memory, footprint, address, size);
        }

        @Override
        public void run() {
            if (freed.compareAndSet(false, true)) {
                memory.free(address, size);
                footprint.add(-size);
            }
        }
    }
}
