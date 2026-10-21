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

import java.util.concurrent.atomic.LongAdder;

/**
 * Allocates through another memory manager and counts the data bytes it holds, so a data
 * structure knows its own footprint. Not thread-safe: for a structure used by one thread at a time.
 */
public final class CountingMemoryManager implements HdMemoryManager {

    private final HdMemoryManager delegate;
    /** Also counts here, for a total over several structures; may be null. */
    private final LongAdder shared;
    private long allocated;

    public CountingMemoryManager(HdMemoryManager delegate) {
        this(delegate, null);
    }

    public CountingMemoryManager(HdMemoryManager delegate, LongAdder shared) {
        this.delegate = delegate;
        this.shared = shared;
    }

    /** Data bytes allocated through this manager and not freed. */
    public long allocated() {
        return allocated;
    }

    @Override
    public long allocate(long size) {
        long address = delegate.allocate(size);
        count(size);
        return address;
    }

    @Override
    public long reallocate(long address, long currentSize, long newSize) {
        long newAddress = delegate.reallocate(address, currentSize, newSize);
        count(newSize - currentSize);
        return newAddress;
    }

    @Override
    public void free(long address, long size) {
        delegate.free(address, size);
        count(-size);
    }

    private void count(long bytes) {
        allocated += bytes;
        if (shared != null) {
            shared.add(bytes);
        }
    }

    @Override
    public long allocateMetadata(long size) {
        return delegate.allocateMetadata(size);
    }

    @Override
    public void freeMetadata(long address, long size) {
        delegate.freeMetadata(address, size);
    }

    @Override
    public long capacity() {
        return delegate.capacity();
    }

    @Override
    public long usedNative() {
        return delegate.usedNative();
    }

    @Override
    public long committedNative() {
        return delegate.committedNative();
    }

    @Override
    public long maxMetadata() {
        return delegate.maxMetadata();
    }

    @Override
    public long usedMetadata() {
        return delegate.usedMetadata();
    }

    @Override
    public void dispose() {
        // the delegate belongs to the member
    }
}
