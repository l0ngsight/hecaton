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
package uk.org.hecaton.hd.memory;

import uk.org.hecaton.memory.NativeOutOfMemoryError;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Capacity accounting and metadata allocation shared by the memory managers.
 */
abstract class AbstractMemoryManager implements HdMemoryManager {

    protected final long capacity;
    protected final long maxMetadata;
    /** Bytes taken from the operating system: data and metadata. */
    private final AtomicLong reserved = new AtomicLong();
    private final AtomicLong usedMetadata = new AtomicLong();

    AbstractMemoryManager(long capacity, long maxMetadata) {
        this.capacity = capacity;
        this.maxMetadata = maxMetadata;
    }

    /**
     * Reserves {@code size} bytes of the capacity before taking them from the operating system.
     *
     * @throws NativeOutOfMemoryError if they do not fit
     */
    protected final void reserve(long size, String what) {
        long current;
        do {
            current = reserved.get();
            if (current + size > capacity) {
                throw new NativeOutOfMemoryError("Not enough native memory for " + what + " of " + size
                        + " bytes: " + current + " of the configured " + capacity + " bytes are in use");
            }
        } while (!reserved.compareAndSet(current, current + size));
    }

    protected final void unreserve(long size) {
        reserved.addAndGet(-size);
    }

    /** Takes {@code size} bytes from the C library, after reserving them. */
    protected final long mallocReserved(long size, String what) {
        reserve(size, what);
        long address = Libc.malloc(size);
        if (address == NULL_ADDRESS) {
            unreserve(size);
            throw new NativeOutOfMemoryError("The operating system has no memory for " + what + " of " + size + " bytes");
        }
        return address;
    }

    @Override
    public long allocateMetadata(long size) {
        long current;
        do {
            current = usedMetadata.get();
            if (current + size > maxMetadata) {
                throw new NativeOutOfMemoryError("Not enough native metadata space for " + size + " bytes: " + current
                        + " of " + maxMetadata + " bytes (the metadata space percentage of the capacity) are in use");
            }
        } while (!usedMetadata.compareAndSet(current, current + size));
        try {
            return mallocReserved(size, "metadata");
        } catch (NativeOutOfMemoryError e) {
            usedMetadata.addAndGet(-size);
            throw e;
        }
    }

    @Override
    public void freeMetadata(long address, long size) {
        Libc.free(address);
        unreserve(size);
        usedMetadata.addAndGet(-size);
    }

    @Override
    public long reallocate(long address, long currentSize, long newSize) {
        long newAddress = allocate(newSize);
        NativeAccess.copy(address, newAddress, Math.min(currentSize, newSize));
        free(address, currentSize);
        return newAddress;
    }

    @Override
    public long capacity() {
        return capacity;
    }

    @Override
    public long maxMetadata() {
        return maxMetadata;
    }

    @Override
    public long usedMetadata() {
        return usedMetadata.get();
    }
}
