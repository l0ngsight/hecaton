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

import uk.org.hecaton.config.NativeMemoryConfig;
import uk.org.hecaton.internal.memory.MemoryAllocator;

/**
 * Allocates the native memory of a member. Data blocks and metadata (hash tables, allocator
 * bookkeeping) share the configured capacity; metadata may use at most the configured metadata
 * space percentage of it. An allocation that does not fit throws
 * {@link uk.org.hecaton.memory.NativeOutOfMemoryError}.
 * <p>
 * Allocation and free are thread-safe. Freeing a block with a size other than the one it was
 * allocated with is undefined.
 */
public interface HdMemoryManager extends MemoryAllocator {

    /** Allocates a metadata block; its contents are undefined. */
    long allocateMetadata(long size);

    void freeMetadata(long address, long size);

    /** The configured capacity in bytes. */
    long capacity();

    /** Bytes in data blocks handed out. */
    long usedNative();

    /** Bytes taken from the operating system for data blocks. */
    long committedNative();

    long maxMetadata();

    long usedMetadata();

    /**
     * Creates the memory manager configured by {@code config}.
     */
    static HdMemoryManager create(NativeMemoryConfig config) {
        long capacity = config.getCapacity().bytes();
        long maxMetadata = (long) (capacity * config.getMetadataSpacePercentage() / 100);
        return switch (config.getAllocatorType()) {
            case STANDARD -> new StandardMemoryManager(capacity, maxMetadata);
            case POOLED -> new PooledMemoryManager(capacity, maxMetadata, config.getPageSize(), config.getMinBlockSize());
        };
    }
}
