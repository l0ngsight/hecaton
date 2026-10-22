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

package uk.org.hecaton.internal.loadprotection;

import uk.org.hecaton.client.impl.protocol.ClientMessage;
import uk.org.hecaton.config.LoadProtectionConfig;
import uk.org.hecaton.internal.metrics.Probe;
import uk.org.hecaton.internal.networking.Channel;
import uk.org.hecaton.internal.networking.nio.ConnectionBufferPool;
import uk.org.hecaton.internal.networking.nio.NioInboundPipeline;
import uk.org.hecaton.internal.networking.nio.NioOutboundPipeline;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.server.tcp.TcpServerConnection;
import uk.org.hecaton.internal.util.counters.MwCounter;
import uk.org.hecaton.logging.ILogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static uk.org.hecaton.internal.metrics.ProbeLevel.MANDATORY;
import static uk.org.hecaton.internal.metrics.ProbeUnit.BYTES;
import static uk.org.hecaton.internal.util.counters.MwCounter.newMwCounter;

/**
 * Admission of client requests and accounting of the data queued for clients (load protection,
 * F23).
 * <ul>
 *     <li>A client connection is read only while it is within its limits: requests in flight and
 *     bytes queued for it. Over them, its decoder stops reading ({@code BLOCKED}), TCP flow control
 *     holds the client back, and {@link #tick()} resumes reading once the connection has drained.</li>
 *     <li>The member stops reading from all client connections while its requests in flight or the
 *     physical bytes queued for clients are over their limits.</li>
 *     <li>While the heap after the last garbage collection is above its threshold, data requests are
 *     answered with {@link uk.org.hecaton.core.HecatonOverloadException} instead of being run.</li>
 *     <li>A connection that does not take any of the data queued for it for the slow consumer
 *     timeout is closed.</li>
 * </ul>
 * Requests in flight are the partition requests (map, queue, ... operations on a key or a
 * partition), which always get a response. Large response frames are counted as physical bytes
 * once per array, however many responses share it.
 */
public final class ClientAdmission {

    /** Frames at least this large are counted as shared arrays. */
    static final int SHARED_FRAME_MIN_BYTES = 64 << 10;
    private static final long SLOW_CONSUMER_SCAN_NANOS = TimeUnit.SECONDS.toNanos(1);

    @Probe(name = "clientRequestsInFlight", level = MANDATORY)
    private final AtomicInteger inFlight = new AtomicInteger();
    @Probe(name = "outboundPhysicalBytes", unit = BYTES, level = MANDATORY)
    private final AtomicLong physicalBytes = new AtomicLong();
    @Probe(name = "outboundSharedLogicalBytes", unit = BYTES, level = MANDATORY)
    private final AtomicLong sharedLogicalBytes = new AtomicLong();
    @Probe(name = "readSuspensions", level = MANDATORY)
    private final MwCounter readSuspensions = newMwCounter();
    @Probe(name = "overloadRejections", level = MANDATORY)
    private final MwCounter overloadRejections = newMwCounter();
    @Probe(name = "slowConsumerDisconnects", level = MANDATORY)
    private final MwCounter slowConsumerDisconnects = newMwCounter();

    private final int maxInFlightPerConnection;
    private final int maxInFlight;
    private final long maxPendingBytesPerConnection;
    private final long maxPhysicalBytes;
    private final long slowConsumerTimeoutNanos;
    private final double heapRejectThreshold;
    private final int maxMessageBytes;
    private final int clientBufferBytes;
    private final ConnectionBufferPool bufferPool;
    private final HeapMonitor heapMonitor;
    private final ILogger logger;

    private final ConcurrentMap<Connection, ConnectionState> connections = new ConcurrentHashMap<>();
    private final Set<ConnectionState> blocked = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<ArrayKey, Integer> sharedArrays = new ConcurrentHashMap<>();
    private long lastSlowConsumerScan = System.nanoTime();

    ClientAdmission(LoadProtectionConfig config, ConnectionBufferPool bufferPool, HeapMonitor heapMonitor, ILogger logger) {
        LoadProtectionConfig.AdmissionConfig admission = config.getAdmissionConfig();
        LoadProtectionConfig.OutboundConfig outbound = config.getOutboundConfig();
        this.maxInFlightPerConnection = admission.getMaxInFlightRequestsPerConnection();
        this.maxInFlight = admission.getMaxInFlightRequests();
        this.heapRejectThreshold = admission.getHeapRejectThreshold();
        this.maxMessageBytes = admission.getMaxMessageBytes();
        this.clientBufferBytes = config.getConnectionBufferConfig().getClientBufferBytes();
        this.bufferPool = bufferPool;
        this.maxPendingBytesPerConnection = outbound.getMaxPendingBytesPerConnection();
        this.maxPhysicalBytes = outbound.getMaxPhysicalPendingBytes();
        this.slowConsumerTimeoutNanos = TimeUnit.SECONDS.toNanos(outbound.getSlowConsumerTimeoutSeconds());
        this.heapMonitor = heapMonitor;
        this.logger = logger;
    }

    /**
     * Whether the decoder of a client connection may decode another request; if not, the
     * connection is registered for {@link #tick()} to resume it, and the decoder returns
     * {@code BLOCKED}.
     */
    public boolean mayRead(Connection connection) {
        ConnectionState state = state(connection);
        if (state.withinLimits()) {
            return true;
        }
        if (blocked.add(state)) {
            readSuspensions.inc();
        }
        return false;
    }

    /** The size of the socket buffers of client connections, unless set by a socket property. */
    public int clientBufferBytes() {
        return clientBufferBytes;
    }

    /**
     * Lets the client connection borrow its socket buffers from the pool while it reads and writes,
     * if pooling is on.
     */
    public void usePooledBuffers(Channel channel) {
        if (bufferPool == null) {
            return;
        }
        if (channel.inboundPipeline() instanceof NioInboundPipeline inbound) {
            inbound.useBufferPool(bufferPool);
        }
        if (channel.outboundPipeline() instanceof NioOutboundPipeline outbound) {
            outbound.useBufferPool(bufferPool);
        }
    }

    /** The largest message read from an authenticated client. */
    public int maxMessageBytes() {
        return maxMessageBytes;
    }

    /** Whether data requests are refused because the heap is above its threshold after garbage collection. */
    public boolean shouldRefuse() {
        boolean refuse = heapRejectThreshold > 0 && heapMonitor.occupancyAfterGc() > heapRejectThreshold;
        if (refuse) {
            overloadRejections.inc();
        }
        return refuse;
    }

    /** Counts a partition request of the connection as in flight until the returned ticket is released. */
    public Runnable admit(Connection connection) {
        ConnectionState state = state(connection);
        state.inFlight.incrementAndGet();
        inFlight.incrementAndGet();
        AtomicBoolean released = new AtomicBoolean();
        return () -> {
            if (released.compareAndSet(false, true)) {
                state.release(1);
            }
        };
    }

    /**
     * Counts the large frames of a response that is about to be queued for the connection, once per
     * array, until the response is written out or the connection closes.
     */
    public void onResponse(Connection connection, ClientMessage response) {
        List<byte[]> large = null;
        for (ClientMessage.ForwardFrameIterator it = response.frameIterator(); it.hasNext(); ) {
            byte[] content = it.next().content;
            if (content != null && content.length >= SHARED_FRAME_MIN_BYTES) {
                if (large == null) {
                    large = new ArrayList<>(1);
                }
                large.add(content);
            }
        }
        if (large == null) {
            return;
        }
        ConnectionState state = state(connection);
        SharedFrames frames = new SharedFrames(large);
        for (byte[] array : large) {
            hold(array);
        }
        state.sharedFrames.put(frames, Boolean.TRUE);
        response.setOnWritten(() -> {
            if (state.sharedFrames.remove(frames) != null) {
                frames.release();
            }
        });
    }

    /** Forgets a closed connection: its requests in flight and its queued frames no longer count. */
    public void connectionRemoved(Connection connection) {
        ConnectionState state = connections.remove(connection);
        if (state == null) {
            return;
        }
        blocked.remove(state);
        state.release(state.inFlight.get());
        for (SharedFrames frames : state.sharedFrames.keySet()) {
            if (state.sharedFrames.remove(frames) != null) {
                frames.release();
            }
        }
    }

    /**
     * Resumes reading from the blocked connections that are within their limits again, and closes
     * slow consumers; called every few milliseconds.
     */
    void tick() {
        for (ConnectionState state : blocked) {
            if (!state.connection.isAlive()) {
                blocked.remove(state);
            } else if (state.withinLimits() && blocked.remove(state)) {
                state.resume();
            }
        }
        long now = System.nanoTime();
        if (now - lastSlowConsumerScan >= SLOW_CONSUMER_SCAN_NANOS) {
            lastSlowConsumerScan = now;
            for (ConnectionState state : connections.values()) {
                if (state.isSlowConsumer(now)) {
                    slowConsumerDisconnects.inc();
                    logger.warning("Closing client connection " + state.connection + ": it did not take any of the "
                            + state.pendingBytes() + " bytes queued for it for "
                            + TimeUnit.NANOSECONDS.toSeconds(slowConsumerTimeoutNanos) + " seconds");
                    state.connection.close("Slow consumer (load protection)", null);
                }
            }
        }
    }

    /** Partition requests of clients in flight on the member. */
    public int inFlight() {
        return inFlight.get();
    }

    /** Distinct bytes of the large response frames queued for clients, each shared array counted once. */
    public long physicalBytes() {
        return physicalBytes.get();
    }

    /** Large response frames queued for clients, each response counted. */
    public long sharedLogicalBytes() {
        return sharedLogicalBytes.get();
    }

    public long readSuspensions() {
        return readSuspensions.get();
    }

    public long overloadRejections() {
        return overloadRejections.get();
    }

    public long slowConsumerDisconnects() {
        return slowConsumerDisconnects.get();
    }

    /** The pool of client socket buffers, or {@code null} if they are not pooled. */
    public ConnectionBufferPool bufferPool() {
        return bufferPool;
    }

    private ConnectionState state(Connection connection) {
        ConnectionState state = connections.get(connection);
        return state != null ? state : connections.computeIfAbsent(connection, ConnectionState::new);
    }

    private void hold(byte[] array) {
        sharedLogicalBytes.addAndGet(array.length);
        // compute() runs atomically per key: the first hold and the last release of an array are exclusive
        sharedArrays.compute(new ArrayKey(array), (key, holders) -> {
            if (holders == null) {
                physicalBytes.addAndGet(array.length);
                return 1;
            }
            return holders + 1;
        });
    }

    private void unhold(byte[] array) {
        sharedLogicalBytes.addAndGet(-array.length);
        sharedArrays.computeIfPresent(new ArrayKey(array), (key, holders) -> {
            if (holders == 1) {
                physicalBytes.addAndGet(-array.length);
                return null;
            }
            return holders - 1;
        });
    }

    /** The large frames of one queued response. */
    private final class SharedFrames {
        private final List<byte[]> arrays;

        SharedFrames(List<byte[]> arrays) {
            this.arrays = arrays;
        }

        void release() {
            for (byte[] array : arrays) {
                unhold(array);
            }
        }
    }

    /** A byte array compared by identity. */
    private record ArrayKey(byte[] array) {
        @Override
        public boolean equals(Object o) {
            return o instanceof ArrayKey that && that.array == array;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(array);
        }
    }

    /** The admission state of one client connection. */
    private final class ConnectionState {
        final Connection connection;
        final AtomicInteger inFlight = new AtomicInteger();
        final Map<SharedFrames, Boolean> sharedFrames = new ConcurrentHashMap<>();
        private long lastBytesWritten = -1;
        private long stalledSince;

        ConnectionState(Connection connection) {
            this.connection = connection;
        }

        boolean withinLimits() {
            return inFlight.get() < maxInFlightPerConnection
                    && pendingBytes() < maxPendingBytesPerConnection
                    && ClientAdmission.this.inFlight.get() < maxInFlight
                    && physicalBytes.get() < maxPhysicalBytes;
        }

        void release(int requests) {
            if (requests > 0) {
                inFlight.addAndGet(-requests);
                ClientAdmission.this.inFlight.addAndGet(-requests);
            }
        }

        long pendingBytes() {
            NioOutboundPipeline outbound = outboundPipeline();
            return outbound == null ? 0 : outbound.pendingBytes();
        }

        void resume() {
            Channel channel = channel();
            if (channel != null && channel.inboundPipeline() instanceof NioInboundPipeline inbound) {
                inbound.wakeup();
            }
        }

        /** Called once a second by the slow consumer scan. */
        boolean isSlowConsumer(long now) {
            NioOutboundPipeline outbound = outboundPipeline();
            if (outbound == null || outbound.pendingBytes() == 0) {
                stalledSince = 0;
                return false;
            }
            long written = outbound.bytesWritten();
            if (written != lastBytesWritten) {
                lastBytesWritten = written;
                stalledSince = now;
                return false;
            }
            return now - stalledSince >= slowConsumerTimeoutNanos;
        }

        private NioOutboundPipeline outboundPipeline() {
            Channel channel = channel();
            return channel != null && channel.outboundPipeline() instanceof NioOutboundPipeline outbound ? outbound : null;
        }

        private Channel channel() {
            return connection instanceof TcpServerConnection tcp ? tcp.getChannel() : null;
        }
    }
}
