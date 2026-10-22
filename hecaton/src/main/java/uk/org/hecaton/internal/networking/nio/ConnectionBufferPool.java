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

package uk.org.hecaton.internal.networking.nio;

import uk.org.hecaton.internal.metrics.Probe;
import uk.org.hecaton.internal.util.counters.MwCounter;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import static uk.org.hecaton.internal.metrics.ProbeLevel.MANDATORY;
import static uk.org.hecaton.internal.metrics.ProbeUnit.BYTES;
import static uk.org.hecaton.internal.util.counters.MwCounter.newMwCounter;

/**
 * The socket buffers of client connections (load protection, F23): a connection borrows its socket
 * read buffer while it reads and its socket write buffer while it writes, and gives them back when
 * they are empty, so that idle connections hold no socket buffers. Returned buffers are kept for
 * reuse up to a number of bytes; beyond it they are left to the garbage collector.
 */
public final class ConnectionBufferPool {

    @Probe(name = "connectionBufferBytesInUse", unit = BYTES, level = MANDATORY)
    private final AtomicLong bytesInUse = new AtomicLong();
    @Probe(name = "connectionBufferBytesIdle", unit = BYTES, level = MANDATORY)
    private final AtomicLong bytesIdle = new AtomicLong();
    @Probe(name = "connectionBufferAllocations", level = MANDATORY)
    private final MwCounter allocations = newMwCounter();

    private final Map<Integer, Queue<ByteBuffer>> idleByCapacity = new ConcurrentHashMap<>();
    private final long maxIdleBytes;
    private final boolean direct;

    public ConnectionBufferPool(long maxIdleBytes, boolean direct) {
        this.maxIdleBytes = maxIdleBytes;
        this.direct = direct;
    }

    /** A cleared buffer of {@code capacity} bytes. */
    ByteBuffer borrow(int capacity) {
        bytesInUse.addAndGet(capacity);
        Queue<ByteBuffer> idle = idleByCapacity.get(capacity);
        ByteBuffer buffer = idle == null ? null : idle.poll();
        if (buffer != null) {
            bytesIdle.addAndGet(-capacity);
            return buffer;
        }
        allocations.inc();
        return direct ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
    }

    /** Takes back an empty buffer; {@code borrowed} if it came from {@link #borrow(int)}, not from its connection. */
    void release(ByteBuffer buffer, boolean borrowed) {
        int capacity = buffer.capacity();
        if (borrowed) {
            bytesInUse.addAndGet(-capacity);
        }
        if (bytesIdle.addAndGet(capacity) <= maxIdleBytes) {
            buffer.clear();
            idleByCapacity.computeIfAbsent(capacity, c -> new ConcurrentLinkedQueue<>()).offer(buffer);
        } else {
            bytesIdle.addAndGet(-capacity);
        }
    }

    /** Stops counting a borrowed buffer that its closed connection keeps. */
    void forget(ByteBuffer buffer) {
        bytesInUse.addAndGet(-buffer.capacity());
    }

    /** Bytes of the buffers that connections have borrowed. */
    public long bytesInUse() {
        return bytesInUse.get();
    }

    /** Bytes of the buffers kept for reuse. */
    public long bytesIdle() {
        return bytesIdle.get();
    }
}
