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
import uk.org.hecaton.internal.networking.OutboundFrame;
import uk.org.hecaton.internal.networking.OutboundHandler;
import uk.org.hecaton.internal.networking.OutboundPipeline;
import uk.org.hecaton.internal.networking.nio.iobalancer.IOBalancer;
import uk.org.hecaton.internal.util.ConcurrencyDetection;
import uk.org.hecaton.internal.util.counters.MwCounter;
import uk.org.hecaton.internal.util.counters.SwCounter;
import uk.org.hecaton.logging.ILogger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_BYTES_WRITTEN;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_IDLE_TIME_MILLIS;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_NORMAL_FRAMES_WRITTEN;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_PRIORITY_FRAMES_WRITTEN;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_PRIORITY_WRITE_QUEUE_PENDING_BYTES;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_PRIORITY_WRITE_QUEUE_SIZE;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_SCHEDULED;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_WRITE_QUEUE_PENDING_BYTES;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_WRITE_QUEUE_SIZE;
import static uk.org.hecaton.internal.metrics.ProbeLevel.DEBUG;
import static uk.org.hecaton.internal.metrics.ProbeLevel.INFO;
import static uk.org.hecaton.internal.metrics.ProbeUnit.BYTES;
import static uk.org.hecaton.internal.metrics.ProbeUnit.MS;
import static uk.org.hecaton.internal.networking.HandlerStatus.CLEAN;
import static uk.org.hecaton.internal.networking.HandlerStatus.DIRTY;
import static uk.org.hecaton.internal.util.EmptyStatement.ignore;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;
import static uk.org.hecaton.internal.util.collection.ArrayUtils.append;
import static uk.org.hecaton.internal.util.collection.ArrayUtils.replaceFirst;
import static uk.org.hecaton.internal.util.counters.MwCounter.newMwCounter;
import static uk.org.hecaton.internal.util.counters.SwCounter.newSwCounter;
import static java.lang.Math.max;
import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.currentThread;
import static java.nio.channels.SelectionKey.OP_WRITE;

public final class NioOutboundPipeline
        extends NioPipeline
        implements Supplier<OutboundFrame>, OutboundPipeline {

    public enum State {
        /*
         * The pipeline isn't scheduled (nothing to do).
         * Only possible next state is scheduled.
         */
        UNSCHEDULED,
        /*
         * The pipeline is scheduled, meaning it is owned by some thread.
         *
         * the next possible states are:
         * - unscheduled (everything got written; we are done)
         * - scheduled: new writes got detected
         * - reschedule: needed if one of the handlers wants to reschedule the pipeline
         */
        SCHEDULED,
        /*
         * One of the handler wants to stop with the pipeline; one of the usages is TLS handshake.
         * Additional writes of frames will not lead to a scheduling of the pipeline. Only a
         * wakeup will schedule the pipeline.
         *
         * Next possible states are:
         * - unscheduled: everything got written
         * - scheduled: new writes got detected
         * - reschedule: pipeline needs to be reprocessed
         * - blocked (one of the handler wants to stop with the pipeline); one of the usages is TLS handshake
         */
        BLOCKED,
        /*
         * state needed for pipeline that was scheduled, but needs to be reprocessed
         * this is needed for wakeup during processing (TLS).
         *
         * Next possible states are:
         * - unscheduled: everything got written
         * - scheduled: new writes got detected
         * - reschedule: pipeline needs to be reprocessed
         * - blocked (one of the handler wants to stop with the pipeline); one of the usages is TLS handshake
         */
        RESCHEDULE
    }

    @SuppressWarnings("checkstyle:visibilitymodifier")
    @Probe(name = NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_WRITE_QUEUE_SIZE, level = INFO)
    public final Queue<OutboundFrame> writeQueue = new ConcurrentLinkedQueue<>();
    @SuppressWarnings("checkstyle:visibilitymodifier")
    @Probe(name = NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_PRIORITY_WRITE_QUEUE_SIZE, level = DEBUG)
    public final Queue<OutboundFrame> priorityWriteQueue = new ConcurrentLinkedQueue<>();

    private OutboundHandler[] handlers = new OutboundHandler[0];
    private ByteBuffer sendBuffer;
    /** Load protection (F23): the pool the socket write buffer is borrowed from while writing, or null. */
    private volatile ConnectionBufferPool bufferPool;
    private int pooledCapacity;
    private boolean bufferBorrowed;

    private final AtomicReference<State> scheduled = new AtomicReference<>(State.SCHEDULED);
    @Probe(name = NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_BYTES_WRITTEN, unit = BYTES, level = DEBUG)
    private final SwCounter bytesWritten = newSwCounter();
    @Probe(name = NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_NORMAL_FRAMES_WRITTEN, level = DEBUG)
    private final SwCounter normalFramesWritten = newSwCounter();
    @Probe(name = NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_PRIORITY_FRAMES_WRITTEN, level = DEBUG)
    private final SwCounter priorityFramesWritten = newSwCounter();

    @Probe(name = NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_WRITE_QUEUE_PENDING_BYTES, level = INFO, unit = BYTES)
    private final MwCounter writeQueuePendingBytes = newMwCounter();
    @Probe(name = NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_PRIORITY_WRITE_QUEUE_PENDING_BYTES, level = INFO, unit = BYTES)
    private final MwCounter priorityWriteQueuePendingBytes = newMwCounter();

    private volatile long lastWriteTime;

    private long bytesWrittenLastPublish;
    private long normalFramesWrittenLastPublish;
    private long priorityFramesWrittenLastPublish;
    private long processCountLastPublish;
    private final ConcurrencyDetection concurrencyDetection;
    private final boolean writeThroughEnabled;
    private final boolean selectionKeyWakeupEnabled;

    NioOutboundPipeline(NioChannel channel,
                        NioThread owner,
                        ChannelErrorHandler errorHandler,
                        ILogger logger,
                        IOBalancer balancer,
                        ConcurrencyDetection concurrencyDetection,
                        boolean writeThroughEnabled,
                        boolean selectionKeyWakeupEnabled) {
        super(channel, owner, errorHandler, OP_WRITE, logger, balancer);
        this.concurrencyDetection = concurrencyDetection;
        this.writeThroughEnabled = writeThroughEnabled;
        this.selectionKeyWakeupEnabled = selectionKeyWakeupEnabled;
    }

    @Override
    public long load() {
        switch (loadType) {
            case LOAD_BALANCING_HANDLE:
                return processCount.get();
            case LOAD_BALANCING_BYTE:
                return bytesWritten.get();
            case LOAD_BALANCING_FRAME:
                return normalFramesWritten.get() + priorityFramesWritten.get();
            default:
                throw new RuntimeException();
        }
    }

    public int totalFramesPending() {
        return writeQueue.size() + priorityWriteQueue.size();
    }

    public long lastWriteTimeMillis() {
        return lastWriteTime;
    }

    /** Bytes of the frames queued and not yet taken by the first handler. */
    public long pendingBytes() {
        return writeQueuePendingBytes.get() + priorityWriteQueuePendingBytes.get();
    }

    @Probe(name = NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_IDLE_TIME_MILLIS, unit = MS, level = INFO)
    private long idleTimeMillis() {
        return max(currentTimeMillis() - lastWriteTime, 0);
    }

    @Probe(name = NETWORKING_METRIC_NIO_OUTBOUND_PIPELINE_SCHEDULED, level = DEBUG)
    private long scheduled() {
        return scheduled.get().ordinal();
    }

    public void write(OutboundFrame frame) {
        if (frame.isUrgent()) {
            if (priorityWriteQueue.offer(frame)) {
                priorityWriteQueuePendingBytes.inc(frame.getFrameLength());
            }
        } else {
            if (writeQueue.offer(frame)) {
                writeQueuePendingBytes.inc(frame.getFrameLength());
            }
        }

        // take care of the scheduling.
        for (; ; ) {
            State state = scheduled.get();
            if (state == State.UNSCHEDULED) {
                // pipeline isn't scheduled, so we need to schedule it.
                if (!scheduled.compareAndSet(State.UNSCHEDULED, State.SCHEDULED)) {
                    // try again
                    continue;
                }

                executePipeline();
                return;
            } else if (state == State.SCHEDULED || state == State.RESCHEDULE) {
                // already scheduled, so we are done
                if (writeThroughEnabled) {
                    concurrencyDetection.onDetected();
                }
                return;
            } else if (state == State.BLOCKED) {
                // pipeline is blocked, so we don't need to schedule
                return;
            } else {
                throw new IllegalStateException("Unexpected state:" + state);
            }
        }
    }

    // executes the pipeline. Either on the calling thread or on the owning NIO thread.
    private void executePipeline() {
        if (writeThroughEnabled && !concurrencyDetection.isDetected()) {
            // we are allowed to do a write through, so lets process the request on the calling thread
            try {
                process();
            } catch (Throwable t) {
                onError(t);
            }
        } else {
            SelectionKey selectionKey = this.selectionKey;
            if (selectionKeyWakeupEnabled && selectionKey != null) {
                try {
                    registerOp(OP_WRITE);
                    selectionKey.selector().wakeup();
                } catch (CancelledKeyException t) {
                    //this means that the selection key is cancelled via another thread, which can happen only
                    //on connection close. Only thing we can do is to ignore the exception. The calling thread could be
                    //user thread and this exception should not be propagated to the user.
                    //From the caller of `uk.org.hecaton.internal.nio.Connection#write`s perspective,
                    // this is the same situation as connection closed after connection.write() successfully returns.
                    ignore(t);
                }
            } else {
                // the owner can be also null during the Pipeline migration, so let's use the helper method
                ownerAddTaskAndWakeup(this);
            }
        }
    }

    @Override
    public OutboundPipeline wakeup() {
        for (; ; ) {
            State prevState = scheduled.get();
            if (prevState == State.RESCHEDULE) {
                break;
            } else {
                if (scheduled.compareAndSet(prevState, State.RESCHEDULE)) {
                    if (prevState == State.UNSCHEDULED || prevState == State.BLOCKED) {
                        ownerAddTaskAndWakeup(this);
                    }
                    break;
                }
            }
        }

        return this;
    }

    @Override
    public OutboundFrame get() {
        OutboundFrame frame = priorityWriteQueue.poll();
        if (frame == null) {
            frame = writeQueue.poll();

            if (frame == null) {
                return null;
            }
            normalFramesWritten.inc();
            writeQueuePendingBytes.inc(-frame.getFrameLength());
        } else {
            priorityFramesWritten.inc();
            priorityWriteQueuePendingBytes.inc(-frame.getFrameLength());
        }

        return frame;
    }

    // is never called concurrently!
    @Override
    @SuppressWarnings("unchecked")
    public void process() throws Exception {
        processCount.inc();
        ConnectionBufferPool pool = bufferPool;
        if (pool != null) {
            borrowSendBuffer(pool);
        }

        OutboundHandler[] localHandlers = handlers;
        HandlerStatus pipelineStatus = CLEAN;
        for (int handlerIndex = 0; handlerIndex < localHandlers.length; handlerIndex++) {
            OutboundHandler handler = localHandlers[handlerIndex];

            HandlerStatus handlerStatus = handler.onWrite();

            if (localHandlers != handlers) {
                // change in the pipeline detected, therefor the loop is restarted.
                localHandlers = handlers;
                pipelineStatus = CLEAN;
                handlerIndex = -1;
            } else if (handlerStatus != CLEAN) {
                pipelineStatus = handlerStatus;
            }
        }

        flushToSocket();

        if (migrationRequested()) {
            startMigration();
            // we leave this method and the NioOutboundPipeline remains scheduled.
            // So we don't need to worry about write-through
            return;
        }

        if (sendBuffer.remaining() > 0) {
            pipelineStatus = DIRTY;
        }

        if (pool != null && pipelineStatus == CLEAN) {
            releaseSendBuffer(pool);
        }

        switch (pipelineStatus) {
            case CLEAN:
                postProcessClean();
                break;
            case DIRTY:
                postProcessDirty();
                break;
            case BLOCKED:
                postProcessBlocked();
                break;
            default:
                throw new IllegalStateException();
        }
    }

    private void postProcessBlocked() {
        // pipeline is blocked; no point in receiving OP_WRITE events.
        unregisterOp(OP_WRITE);

        // we try to set the state to blocked.
        for (; ; ) {
            State state = scheduled.get();
            if (state == State.SCHEDULED) {
                // if it is still scheduled, we'll just try to put it to blocked.
                if (scheduled.compareAndSet(State.SCHEDULED, State.BLOCKED)) {
                    break;
                }
            } else if (state == State.BLOCKED) {
                // it is already blocked, so we are done.
                break;
            } else if (state == State.RESCHEDULE) {
                // rescheduling is requested, so let's do that. Once put to RESCHEDULE,
                // only the thread running the process method will change the state, so we can safely call a set.
                scheduled.set(State.SCHEDULED);
                // this will cause the pipeline to be rescheduled.
                owner().addTaskAndWakeup(this);
                break;
            } else {
                throw new IllegalStateException("unexpected state:" + state);
            }
        }
    }

    private void postProcessDirty() {
        // pipeline is dirty, so register for an OP_WRITE to write more data.
        registerOp(OP_WRITE);

        if (writeThroughEnabled && !(Thread.currentThread() instanceof NioThread)) {
            // there was a write through. Changing the interested set of the selection key
            // after the IO thread did a select, will not lead to the selector waking up. So
            // if we don't wake up the selector explicitly, only after the selector.select(timeout)
            // has expired the selectionKey will be seen. For more info see:
            // https://stackoverflow.com/questions/11523471/java-selectionkey-interestopsint-not-thread-safe
            owner.getSelector().wakeup();
            concurrencyDetection.onDetected();
        }
    }

    private void postProcessClean() {
        // There is nothing left to be done; so lets unschedule this pipeline
        // since everything is written, we are not interested anymore in write-events, so lets unsubscribe
        unregisterOp(OP_WRITE);

        for (; ; ) {
            State state = scheduled.get();
            if (state == State.RESCHEDULE) {
                // the pipeline needs to be rescheduled. The current thread is still owner of the pipeline,
                // so lets remove the reschedule flag and return it to schedule and let's reprocess the pipeline.
                scheduled.set(State.SCHEDULED);
                owner().addTaskAndWakeup(this);
                return;
            }

            if (!scheduled.compareAndSet(state, State.UNSCHEDULED)) {
                // we didn't manage to set it to unscheduled, lets retry the loop and see what needs to be done
                continue;
            }

            // we manage to unschedule the pipeline. From this point on we have released ownership of the pipeline
            // and another thread could call the process method.
            if (writeQueue.isEmpty() && priorityWriteQueue.isEmpty()) {
                //pipeline is clean, we are done.
                return;
            }

            // there is stuff to write we are going to reclaim ownership of the pipeline to prevent
            // we are going to end up with scheduled pipeline that isn't going to be processed.
            // If we can't reclaim ownership, then it is the concern of the other thread deal with the pipeline.
            if (scheduled.compareAndSet(State.UNSCHEDULED, State.SCHEDULED)) {
                if (Thread.currentThread().getClass() == NioThread.class) {
                    owner().addTask(this);
                } else {
                    owner().addTaskAndWakeup(this);
                }
            }

            return;
        }
    }

    /**
     * Borrows the socket write buffer from {@code pool} while writing and gives it back when it has
     * been flushed, so that the connection holds no write buffer while idle (load protection, F23).
     */
    public void useBufferPool(ConnectionBufferPool pool) {
        this.bufferPool = pool;
    }

    private void borrowSendBuffer(ConnectionBufferPool pool) {
        OutboundHandler last = handlers[handlers.length - 1];
        if (last.dst() == null) {
            ByteBuffer buffer = pool.borrow(pooledCapacity);
            bufferBorrowed = true;
            // the handlers' dst is in reading mode between calls: an empty one has no bytes to read
            buffer.flip();
            last.dst(buffer);
            sendBuffer = buffer;
        }
    }

    private void releaseSendBuffer(ConnectionBufferPool pool) {
        OutboundHandler last = handlers[handlers.length - 1];
        if (last.dst() instanceof ByteBuffer buffer && !buffer.hasRemaining()) {
            pooledCapacity = buffer.capacity();
            last.dst(null);
            sendBuffer = null;
            pool.release(buffer, bufferBorrowed);
            bufferBorrowed = false;
        }
    }

    /** Stops the pool counting the write buffer of the closed connection. */
    void forgetPooledBuffer() {
        ConnectionBufferPool pool = bufferPool;
        OutboundHandler[] localHandlers = handlers;
        if (pool != null && bufferBorrowed && localHandlers.length > 0
                && localHandlers[localHandlers.length - 1].dst() instanceof ByteBuffer buffer) {
            pool.forget(buffer);
        }
    }

    private void flushToSocket() throws IOException {
        lastWriteTime = currentTimeMillis();
        int written = socketChannel.write(sendBuffer);
        bytesWritten.inc(written);
        //System.out.println(channel + " bytes written:" + written);
    }

    void drainWriteQueues() {
        writeQueue.clear();
        priorityWriteQueue.clear();
        writeQueuePendingBytes.set(0);
        priorityWriteQueuePendingBytes.set(0);
    }

    /** Bytes written to the socket so far. */
    public long bytesWritten() {
        return bytesWritten.get();
    }

    @Override
    protected void publishMetrics() {
        if (currentThread() != owner) {
            return;
        }

        owner.bytesTransceived += bytesWritten.get() - bytesWrittenLastPublish;
        owner.framesTransceived += normalFramesWritten.get() - normalFramesWrittenLastPublish;
        owner.priorityFramesTransceived += priorityFramesWritten.get() - priorityFramesWrittenLastPublish;
        owner.processCount += processCount.get() - processCountLastPublish;

        bytesWrittenLastPublish = bytesWritten.get();
        normalFramesWrittenLastPublish = normalFramesWritten.get();
        priorityFramesWrittenLastPublish = priorityFramesWritten.get();
        processCountLastPublish = processCount.get();
    }

    @Override
    public String toString() {
        return channel + ".outboundPipeline";
    }

    @Override
    protected Iterable<? extends ChannelHandler> handlers() {
        return Arrays.asList(handlers);
    }

    @Override
    public OutboundPipeline remove(OutboundHandler handler) {
        return replace(handler);
    }

    @Override
    public OutboundPipeline addLast(OutboundHandler... addedHandlers) {
        checkNotNull(addedHandlers, "addedHandlers can't be null");

        for (OutboundHandler addedHandler : addedHandlers) {
            addedHandler.setChannel(channel).handlerAdded();
        }
        updatePipeline(append(handlers, addedHandlers));
        return this;
    }

    @Override
    public OutboundPipeline replace(OutboundHandler oldHandler, OutboundHandler... addedHandlers) {
        checkNotNull(oldHandler, "oldHandler can't be null");
        checkNotNull(addedHandlers, "newHandler can't be null");

        OutboundHandler[] newHandlers = replaceFirst(handlers, oldHandler, addedHandlers);
        if (newHandlers == handlers) {
            throw new IllegalArgumentException("handler " + oldHandler + " isn't part of the pipeline");
        }

        for (OutboundHandler addedHandler : addedHandlers) {
            addedHandler.setChannel(channel).handlerAdded();
        }
        updatePipeline(newHandlers);
        return this;
    }

    private void updatePipeline(OutboundHandler[] newHandlers) {
        if (bufferBorrowed) {
            // the replaced handler keeps the buffer it borrowed
            ConnectionBufferPool pool = bufferPool;
            if (pool != null && sendBuffer != null) {
                pool.forget(sendBuffer);
            }
            bufferBorrowed = false;
        }
        this.handlers = newHandlers;
        this.sendBuffer = newHandlers.length == 0 ? null : (ByteBuffer) newHandlers[newHandlers.length - 1].dst();

        OutboundHandler prev = null;
        for (OutboundHandler handler : handlers) {
            if (prev == null) {
                handler.src(this);
            } else {
                Object src = prev.dst();
                if (src instanceof ByteBuffer) {
                    handler.src(src);
                }
            }
            prev = handler;
        }
    }

    // useful for debugging
//    private String pipelineToString() {
//        StringBuilder sb = new StringBuilder("out-pipeline[");
//        OutboundHandler[] handlers = this.handlers;
//        for (int k = 0; k < handlers.length; k++) {
//            if (k > 0) {
//                sb.append("->-");
//            }
//            sb.append(handlers[k].getClass().getSimpleName());
//        }
//        sb.append(']');
//        return sb.toString();
//    }
}
