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

import java.util.concurrent.atomic.AtomicLong;

/**
 * The {@code STANDARD} allocator: every block comes from the C library's {@code malloc}.
 */
final class StandardMemoryManager extends AbstractMemoryManager {

    private final AtomicLong used = new AtomicLong();

    StandardMemoryManager(long capacity, long maxMetadata) {
        super(capacity, maxMetadata);
    }

    @Override
    public long allocate(long size) {
        long address = mallocReserved(size, "a block");
        used.addAndGet(size);
        return address;
    }

    @Override
    public void free(long address, long size) {
        Libc.free(address);
        unreserve(size);
        used.addAndGet(-size);
    }

    @Override
    public long usedNative() {
        return used.get();
    }

    @Override
    public long committedNative() {
        return used.get();
    }

    @Override
    public void dispose() {
        // blocks belong to the data structures, which free them when they are destroyed
    }
}
