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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static uk.org.hecaton.internal.util.QuickMath.isPowerOfTwo;

/**
 * The {@code POOLED} allocator: a buddy allocator over pages of {@code pageSize} bytes, taken from
 * the C library aligned to their size. Blocks are powers of two from {@code minBlockSize} to the
 * page size; larger allocations come from the C library directly.
 * <p>
 * Each thread allocates from its own pool, so pools are rarely contended; a block freed by another
 * thread goes back to the pool that owns its page. Free blocks are kept in per-size doubly linked
 * lists threaded through the blocks themselves; which blocks are free is recorded in a bitmap per
 * page, kept in metadata space (about {@code 2 / minBlockSize} bits per byte of page).
 */
final class PooledMemoryManager extends AbstractMemoryManager {

    private static final long NEXT = 0;
    private static final long PREV = 8;
    /** A free block holds its list's next and previous pointers. */
    private static final int MIN_BLOCK_SIZE = 16;
    private static final int BITS_PER_LONG_SHIFT = 6;
    private static final int BITS_PER_LONG = 1 << BITS_PER_LONG_SHIFT;

    private final long pageSize;
    private final long minBlockSize;
    private final int minShift;
    /** Order of a whole page: block size = minBlockSize << order. */
    private final int maxOrder;
    /** Offset, in bits, of each order's bitmap within a page's bitmap. */
    private final long[] bitmapOffsets;
    private final long bitmapBytes;

    private final Map<Long, Page> pages = new ConcurrentHashMap<>();
    private final List<Pool> allPools = new CopyOnWriteArrayList<>();
    private final ThreadLocal<Pool> pools = ThreadLocal.withInitial(() -> {
        Pool pool = new Pool();
        allPools.add(pool);
        return pool;
    });
    private final AtomicLong used = new AtomicLong();
    private final AtomicLong committed = new AtomicLong();

    PooledMemoryManager(long capacity, long maxMetadata, int pageSize, int minBlockSize) {
        super(capacity, maxMetadata);
        if (!isPowerOfTwo(pageSize) || !isPowerOfTwo(minBlockSize) || minBlockSize < MIN_BLOCK_SIZE || minBlockSize > pageSize) {
            throw new IllegalArgumentException("Page size (" + pageSize + ") and minimum block size (" + minBlockSize
                    + ") must be powers of two, with 16 <= minimum block size <= page size");
        }
        this.pageSize = pageSize;
        this.minBlockSize = minBlockSize;
        this.minShift = Long.numberOfTrailingZeros(minBlockSize);
        this.maxOrder = Long.numberOfTrailingZeros(pageSize) - minShift;
        this.bitmapOffsets = new long[maxOrder + 1];
        long bits = 0;
        for (int order = 0; order <= maxOrder; order++) {
            bitmapOffsets[order] = bits;
            bits += pageSize >>> (minShift + order);
        }
        this.bitmapBytes = ((bits + BITS_PER_LONG - 1) >>> BITS_PER_LONG_SHIFT) * Long.BYTES;
    }

    @Override
    public long allocate(long size) {
        if (size > pageSize) {
            long address = mallocReserved(size, "a block");
            used.addAndGet(size);
            committed.addAndGet(size);
            return address;
        }
        int order = orderOf(size);
        Pool own = pools.get();
        long address;
        try {
            address = own.allocate(order);
        } catch (NativeOutOfMemoryError e) {
            // no room for a new page: take a free block from another thread's pages
            address = stealFromOtherPools(own, order);
            if (address == NULL_ADDRESS) {
                throw e;
            }
        }
        used.addAndGet(minBlockSize << order);
        return address;
    }

    private long stealFromOtherPools(Pool own, int order) {
        for (Pool pool : allPools) {
            if (pool != own) {
                long address = pool.allocateFromFreeLists(order);
                if (address != NULL_ADDRESS) {
                    return address;
                }
            }
        }
        return NULL_ADDRESS;
    }

    @Override
    public void free(long address, long size) {
        if (size > pageSize) {
            Libc.free(address);
            unreserve(size);
            used.addAndGet(-size);
            committed.addAndGet(-size);
            return;
        }
        int order = orderOf(size);
        Page page = pages.get(address & -pageSize);
        if (page == null) {
            throw new IllegalArgumentException("Address " + address + " was not allocated by this allocator");
        }
        page.pool.free(page, address, order);
        used.addAndGet(-(minBlockSize << order));
    }

    @Override
    public long reallocate(long address, long currentSize, long newSize) {
        if (currentSize <= pageSize && newSize <= pageSize && orderOf(currentSize) == orderOf(newSize)) {
            return address;
        }
        return super.reallocate(address, currentSize, newSize);
    }

    int orderOf(long size) {
        if (size <= minBlockSize) {
            return 0;
        }
        return Long.SIZE - Long.numberOfLeadingZeros(size - 1) - minShift;
    }

    @Override
    public long usedNative() {
        return used.get();
    }

    @Override
    public long committedNative() {
        return committed.get();
    }

    /** Pages currently taken from the C library, for tests. */
    int pageCount() {
        return pages.size();
    }

    @Override
    public void dispose() {
        for (Page page : pages.values()) {
            page.pool.lock.lock();
            try {
                if (pages.remove(page.address) != null) {
                    releasePage(page);
                }
            } finally {
                page.pool.lock.unlock();
            }
        }
    }

    private void releasePage(Page page) {
        Libc.free(page.address);
        unreserve(pageSize);
        committed.addAndGet(-pageSize);
        freeMetadata(page.bitmap, bitmapBytes);
    }

    private record Page(long address, long bitmap, Pool pool) {
    }

    /** The pages and free lists of one thread. */
    private final class Pool {

        final ReentrantLock lock = new ReentrantLock();
        /** Head of the free list of each order, 0 if empty. */
        final long[] freeLists = new long[maxOrder + 1];
        int freePages;

        /** A block from this pool's free lists, without taking a new page; 0 if there is none. */
        long allocateFromFreeLists(int order) {
            lock.lock();
            try {
                int available = order;
                while (available <= maxOrder && freeLists[available] == NULL_ADDRESS) {
                    available++;
                }
                if (available > maxOrder) {
                    return NULL_ADDRESS;
                }
                long block = freeLists[available];
                Page page = pages.get(block & -pageSize);
                unlink(page, block, available);
                while (available > order) {
                    available--;
                    push(page, block + (minBlockSize << available), available);
                }
                return block;
            } finally {
                lock.unlock();
            }
        }

        long allocate(int order) {
            lock.lock();
            try {
                long block = allocateFromFreeLists(order);
                if (block != NULL_ADDRESS) {
                    return block;
                }
                Page page = newPage();
                block = page.address;
                // split the page until the block has the requested order; the upper halves become free
                for (int available = maxOrder; available > order; ) {
                    available--;
                    push(page, block + (minBlockSize << available), available);
                }
                return block;
            } finally {
                lock.unlock();
            }
        }

        void free(Page page, long block, int order) {
            lock.lock();
            try {
                long address = block;
                int current = order;
                while (current < maxOrder) {
                    long buddy = page.address + ((address - page.address) ^ (minBlockSize << current));
                    if (!isFree(page, buddy, current)) {
                        break;
                    }
                    unlink(page, buddy, current);
                    address = Math.min(address, buddy);
                    current++;
                }
                if (current == maxOrder && freePages > 0) {
                    // keep one free page per pool, return the others to the C library
                    pages.remove(page.address);
                    releasePage(page);
                    return;
                }
                push(page, address, current);
            } finally {
                lock.unlock();
            }
        }

        private Page newPage() {
            reserve(pageSize, "a page");
            long address = Libc.alignedAlloc(pageSize, pageSize);
            if (address == NULL_ADDRESS) {
                unreserve(pageSize);
                throw new NativeOutOfMemoryError("The operating system has no memory for a page of " + pageSize + " bytes");
            }
            long bitmap;
            try {
                bitmap = allocateMetadata(bitmapBytes);
            } catch (NativeOutOfMemoryError e) {
                Libc.free(address);
                unreserve(pageSize);
                throw e;
            }
            NativeAccess.fill(bitmap, bitmapBytes, (byte) 0);
            committed.addAndGet(pageSize);
            Page page = new Page(address, bitmap, this);
            pages.put(address, page);
            return page;
        }

        private void push(Page page, long block, int order) {
            long head = freeLists[order];
            NativeAccess.putLong(block + NEXT, head);
            NativeAccess.putLong(block + PREV, NULL_ADDRESS);
            if (head != NULL_ADDRESS) {
                NativeAccess.putLong(head + PREV, block);
            }
            freeLists[order] = block;
            setFree(page, block, order, true);
            if (order == maxOrder) {
                freePages++;
            }
        }

        private void unlink(Page page, long block, int order) {
            long next = NativeAccess.getLong(block + NEXT);
            long prev = NativeAccess.getLong(block + PREV);
            if (prev == NULL_ADDRESS) {
                freeLists[order] = next;
            } else {
                NativeAccess.putLong(prev + NEXT, next);
            }
            if (next != NULL_ADDRESS) {
                NativeAccess.putLong(next + PREV, prev);
            }
            setFree(page, block, order, false);
            if (order == maxOrder) {
                freePages--;
            }
        }

        private boolean isFree(Page page, long block, int order) {
            long bit = bitIndex(page, block, order);
            return (NativeAccess.getLong(page.bitmap + (bit >>> BITS_PER_LONG_SHIFT) * Long.BYTES) & (1L << bit)) != 0;
        }

        private void setFree(Page page, long block, int order, boolean free) {
            long bit = bitIndex(page, block, order);
            long word = page.bitmap + (bit >>> BITS_PER_LONG_SHIFT) * Long.BYTES;
            long value = NativeAccess.getLong(word);
            NativeAccess.putLong(word, free ? value | (1L << bit) : value & ~(1L << bit));
        }

        private long bitIndex(Page page, long block, int order) {
            return bitmapOffsets[order] + ((block - page.address) >>> (minShift + order));
        }
    }

    @Override
    public String toString() {
        return "PooledMemoryManager{pageSize=" + pageSize + ", minBlockSize=" + minBlockSize + ", pages=" + pages.size()
                + ", used=" + used.get() + ", committed=" + committed.get() + '}';
    }
}
