/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
import uk.org.hecaton.internal.networking.ChannelErrorHandler;
import uk.org.hecaton.internal.networking.ChannelHandler;
import uk.org.hecaton.internal.networking.HandlerStatus;
import uk.org.hecaton.internal.networking.InboundHandler;
import uk.org.hecaton.internal.networking.InboundPipeline;
import uk.org.hecaton.internal.networking.nio.iobalancer.IOBalancer;
import uk.org.hecaton.internal.util.counters.SwCounter;
import uk.org.hecaton.logging.ILogger;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.util.Arrays;

import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_INBOUND_PIPELINE_BYTES_READ;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_INBOUND_PIPELINE_IDLE_TIME_MS;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_INBOUND_PIPELINE_NORMAL_FRAMES_READ;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_INBOUND_PIPELINE_PRIORITY_FRAMES_READ;
import static uk.org.hecaton.internal.metrics.ProbeLevel.DEBUG;
import static uk.org.hecaton.internal.metrics.ProbeUnit.BYTES;
import static uk.org.hecaton.internal.metrics.ProbeUnit.MS;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;
import static uk.org.hecaton.internal.util.collection.ArrayUtils.append;
import static uk.org.hecaton.internal.util.collection.ArrayUtils.replaceFirst;
import static uk.org.hecaton.internal.util.counters.SwCounter.newSwCounter;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.nio.channels.SelectionKey.OP_READ;

/**
 * When the {@link NioThread} receives a read event from the
 * {@link Selector}, then the {@link #process()} is called to read
 * out the data from the socket into a bytebuffer and hand it over to the
 * {@link InboundHandler} to get processed.
 */
public final class NioInboundPipeline extends NioPipeline implements InboundPipeline {

    private InboundHandler[] handlers = new InboundHandler[0];
    private ByteBuffer receiveBuffer;
    /** Load protection (F23): the pool the socket read buffer is borrowed from while reading, or null. */
    private volatile ConnectionBufferPool bufferPool;
    private int pooledCapacity;
    private boolean bufferBorrowed;

    @Probe(name = NETWORKING_METRIC_NIO_INBOUND_PIPELINE_BYTES_READ, unit = BYTES, level = DEBUG)
    private final SwCounter bytesRead = newSwCounter();
    @Probe(name = NETWORKING_METRIC_NIO_INBOUND_PIPELINE_NORMAL_FRAMES_READ, level = DEBUG)
    private final SwCounter normalFramesRead = newSwCounter();
    @Probe(name = NETWORKING_METRIC_NIO_INBOUND_PIPELINE_PRIORITY_FRAMES_READ, level = DEBUG)
    private final SwCounter priorityFramesRead = newSwCounter();
    private volatile long lastReadTime = -1;

    private volatile long bytesReadLastPublish;
    private volatile long normalFramesReadLastPublish;
    private volatile long priorityFramesReadLastPublish;
    private volatile long processCountLastPublish;

    NioInboundPipeline(NioChannel channel,
                       NioThread owner,
                       ChannelErrorHandler errorHandler,
                       ILogger logger,
                       IOBalancer balancer) {
        super(channel, owner, errorHandler, OP_READ, logger, balancer);
    }

    public long normalFramesRead() {
        return normalFramesRead.get();
    }

    public long priorityFramesRead() {
        return priorityFramesRead.get();
    }

    @Override
    public long load() {
        switch (loadType) {
            case LOAD_BALANCING_HANDLE:
                return processCount.get();
            case LOAD_BALANCING_BYTE:
                return bytesRead.get();
            case LOAD_BALANCING_FRAME:
                return normalFramesRead.get() + priorityFramesRead.get();
            default:
                throw new RuntimeException();
        }
    }

    @Probe(name = NETWORKING_METRIC_NIO_INBOUND_PIPELINE_IDLE_TIME_MS, unit = MS, level = DEBUG)
    private long idleTimeMillis() {
        return Math.max(currentTimeMillis() - lastReadTime, 0);
    }

    public long lastReadTimeMillis() {
        return lastReadTime;
    }

    /**
     * Borrows the socket read buffer from {@code pool} while reading and gives it back when it is
     * empty, so that the connection holds no read buffer while idle (load protection, F23).
     */
    public void useBufferPool(ConnectionBufferPool pool) {
        this.bufferPool = pool;
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    void process() throws Exception {
        ConnectionBufferPool pool = bufferPool;
        if (pool != null) {
            borrowReceiveBuffer(pool);
        }
        int readBytes = socketChannel.read(receiveBuffer);

        if (readBytes == -1) {
            throw new EOFException("Remote socket closed!");
        }

        if (readBytes > 0) {
            processCount.inc();
            lastReadTime = currentTimeMillis();
            bytesRead.inc(readBytes);
        }

        // currently the whole pipeline is retried when one of the handlers is dirty; but only the dirty handler
        // and the remaining sequence should need to retry.
        InboundHandler[] localHandlers = handlers;
        boolean cleanPipeline;
        boolean unregisterRead;
        do {
            cleanPipeline = true;
            unregisterRead = false;
            for (int handlerIndex = 0; handlerIndex < localHandlers.length; handlerIndex++) {
                InboundHandler handler = localHandlers[handlerIndex];
                HandlerStatus handlerStatus = handler.onRead();
                if (localHandlers != handlers) {
                    // change in the pipeline detected, restarting loop
                    handlerIndex = -1;
                    localHandlers = handlers;
                    continue;
                }

                switch (handlerStatus) {
                    case CLEAN:
                        break;
                    case DIRTY:
                        cleanPipeline = false;
                        break;
                    case BLOCKED:
                        // setting cleanPipeline to true keep flushing everything downstream, but not upstream.
                        cleanPipeline = true;
                        unregisterRead = true;
                        break;
                    default:
                        throw new IllegalStateException();
                }
            }
        } while (!cleanPipeline);

        if (migrationRequested()) {
            startMigration();
            return;
        }

        if (unregisterRead) {
            unregisterOp(OP_READ);
        }
        if (pool != null) {
            releaseReceiveBufferIfEmpty(pool);
        }
    }

    private void borrowReceiveBuffer(ConnectionBufferPool pool) {
        InboundHandler first = handlers[0];
        if (first.src() == null) {
            ByteBuffer buffer = pool.borrow(pooledCapacity);
            bufferBorrowed = true;
            first.src(buffer);
            receiveBuffer = buffer;
        }
    }

    private void releaseReceiveBufferIfEmpty(ConnectionBufferPool pool) {
        InboundHandler first = handlers[0];
        if (first.src() instanceof ByteBuffer buffer && buffer.position() == 0) {
            pooledCapacity = buffer.capacity();
            first.src(null);
            receiveBuffer = null;
            pool.release(buffer, bufferBorrowed);
            bufferBorrowed = false;
        }
    }

    /** Stops the pool counting the read buffer of the closed connection. */
    void forgetPooledBuffer() {
        ConnectionBufferPool pool = bufferPool;
        InboundHandler[] localHandlers = handlers;
        if (pool != null && bufferBorrowed && localHandlers.length > 0
                && localHandlers[0].src() instanceof ByteBuffer buffer) {
            pool.forget(buffer);
        }
    }

    long bytesRead() {
        return bytesRead.get();
    }

    @Override
    void publishMetrics() {
        if (currentThread() != owner) {
            return;
        }
        // since this is executed by the owner, the owner field can't change while
        // this method is executed.
        owner.bytesTransceived += bytesRead.get() - bytesReadLastPublish;
        owner.framesTransceived += normalFramesRead.get() - normalFramesReadLastPublish;
        owner.priorityFramesTransceived += priorityFramesRead.get() - priorityFramesReadLastPublish;
        owner.processCount += processCount.get() - processCountLastPublish;

        bytesReadLastPublish = bytesRead.get();
        normalFramesReadLastPublish = normalFramesRead.get();
        priorityFramesReadLastPublish = priorityFramesRead.get();
        processCountLastPublish = processCount.get();
    }

    @Override
    public String toString() {
        return channel + ".inboundPipeline";
    }

    @Override
    protected Iterable<? extends ChannelHandler> handlers() {
        return Arrays.asList(handlers);
    }

    @Override
    public InboundPipeline remove(InboundHandler handler) {
        return replace(handler);
    }

    @Override
    public InboundPipeline addLast(InboundHandler... addedHandlers) {
        checkNotNull(addedHandlers, "handlers can't be null");

        for (InboundHandler addedHandler : addedHandlers) {
            fixDependencies(addedHandler);
            addedHandler.setChannel(channel).handlerAdded();
        }

        updatePipeline(append(handlers, addedHandlers));
        return this;
    }

    @Override
    public InboundPipeline replace(InboundHandler oldHandler, InboundHandler... addedHandlers) {
        checkNotNull(oldHandler, "oldHandler can't be null");
        checkNotNull(addedHandlers, "addedHandlers can't be null");

        InboundHandler[] newHandlers = replaceFirst(handlers, oldHandler, addedHandlers);
        if (newHandlers == handlers) {
            throw new IllegalArgumentException("handler " + oldHandler + " isn't part of the pipeline");
        }

        for (InboundHandler addedHandler : addedHandlers) {
            fixDependencies(addedHandler);
            addedHandler.setChannel(channel).handlerAdded();
        }
        updatePipeline(newHandlers);
        return this;
    }

    private void fixDependencies(ChannelHandler addedHandler) {
        if (addedHandler instanceof InboundHandlerWithCounters c) {
            c.setNormalPacketsRead(normalFramesRead);
            c.setPriorityPacketsRead(priorityFramesRead);
        }
    }

    private void updatePipeline(InboundHandler[] handlers) {
        if (bufferBorrowed) {
            // the replaced handler keeps the buffer it borrowed
            ConnectionBufferPool pool = bufferPool;
            if (pool != null && receiveBuffer != null) {
                pool.forget(receiveBuffer);
            }
            bufferBorrowed = false;
        }
        this.handlers = handlers;
        receiveBuffer = handlers.length == 0 ? null : (ByteBuffer) handlers[0].src();

        InboundHandler prev = null;
        for (InboundHandler handler : handlers) {
            if (prev != null) {
                Object src = handler.src();
                if (src instanceof ByteBuffer) {
                    prev.dst(src);
                }
            }
            prev = handler;
        }
    }

    // useful for debugging
//    private String pipelineToString() {
//        StringBuilder sb = new StringBuilder("in-pipeline[");
//        InboundHandler[] handlers = this.handlers;
//        for (int k = 0; k < handlers.length; k++) {
//            if (k > 0) {
//                sb.append("->-");
//            }
//            sb.append(handlers[k].getClass().getSimpleName());
//        }
//        sb.append(']');
//        return sb.toString();
//    }

    @Override
    public NioInboundPipeline wakeup() {
        ownerAddTaskAndWakeup(new NioPipelineTask(this) {
            @Override
            protected void run0() {
                registerOp(OP_READ);
                NioInboundPipeline.this.run();
            }
        });

        return this;
    }
}
