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

package uk.org.hecaton.config;

import java.util.Objects;

import static uk.org.hecaton.internal.util.Preconditions.checkNotNegative;
import static uk.org.hecaton.internal.util.Preconditions.checkPositive;

/**
 * Load protection (F23): keeps a member's memory bounded when many readers want the same large
 * values, and keeps connection churn away from map and queue operations. One switch turns it on;
 * with it off (the default) none of it is active and the member behaves as without it.
 * <p>
 * With load protection on:
 * <ul>
 *     <li>maps with {@link MapConfig#setImmutable(boolean) immutable} set never update a held
 *     value, and their values are shared instead of copied: on the member that owns them, with
 *     the client responses, and on the other members through a near cache;</li>
 *     <li>outbound data, in-flight client requests and client connection buffers are bounded
 *     ({@link #getOutboundConfig()}, {@link #getAdmissionConfig()},
 *     {@link #getConnectionBufferConfig()});</li>
 *     <li>client disconnect cleanup runs on its own threads, in batches, and new connections are
 *     admitted at a bounded rate ({@link #getMaintenanceConfig()});</li>
 *     <li>queues with {@link QueueConfig#setShardCount(int) shards} spread over several
 *     partitions.</li>
 * </ul>
 * The property {@code hecaton.load-protection.enabled} overrides {@link #setEnabled(boolean)}, so
 * operators can turn the feature off without changing code.
 */
public class LoadProtectionConfig {

    /** Default for {@link #setImmutableMapNearCacheSize(int)}. */
    public static final int DEFAULT_IMMUTABLE_MAP_NEAR_CACHE_SIZE = 1_000;

    private boolean enabled;
    private int immutableMapNearCacheSize = DEFAULT_IMMUTABLE_MAP_NEAR_CACHE_SIZE;
    private OutboundConfig outboundConfig = new OutboundConfig();
    private AdmissionConfig admissionConfig = new AdmissionConfig();
    private MaintenanceConfig maintenanceConfig = new MaintenanceConfig();
    private ConnectionBufferConfig connectionBufferConfig = new ConnectionBufferConfig();

    public LoadProtectionConfig() {
    }

    public LoadProtectionConfig(LoadProtectionConfig config) {
        enabled = config.enabled;
        immutableMapNearCacheSize = config.immutableMapNearCacheSize;
        outboundConfig = new OutboundConfig(config.outboundConfig);
        admissionConfig = new AdmissionConfig(config.admissionConfig);
        maintenanceConfig = new MaintenanceConfig(config.maintenanceConfig);
        connectionBufferConfig = new ConnectionBufferConfig(config.connectionBufferConfig);
    }

    /** Whether load protection is on. */
    public boolean isEnabled() {
        return enabled;
    }

    /** Turns load protection on or off; it is read when the member starts. */
    public LoadProtectionConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Entries of the near cache that each member keeps for an immutable map that has no near cache
     * configured. The near cache holds deserialised values (in-memory format {@code OBJECT}),
     * evicts the least recently used entries, and is shared by all readers on the member.
     */
    public int getImmutableMapNearCacheSize() {
        return immutableMapNearCacheSize;
    }

    /**
     * Sets the entries of the near cache of an immutable map on each member; {@code 0} gives
     * immutable maps no near cache unless one is configured on the map.
     */
    public LoadProtectionConfig setImmutableMapNearCacheSize(int immutableMapNearCacheSize) {
        this.immutableMapNearCacheSize = checkNotNegative(immutableMapNearCacheSize,
                "immutableMapNearCacheSize must not be negative");
        return this;
    }

    public OutboundConfig getOutboundConfig() {
        return outboundConfig;
    }

    public LoadProtectionConfig setOutboundConfig(OutboundConfig outboundConfig) {
        this.outboundConfig = Objects.requireNonNull(outboundConfig, "outboundConfig");
        return this;
    }

    public AdmissionConfig getAdmissionConfig() {
        return admissionConfig;
    }

    public LoadProtectionConfig setAdmissionConfig(AdmissionConfig admissionConfig) {
        this.admissionConfig = Objects.requireNonNull(admissionConfig, "admissionConfig");
        return this;
    }

    public MaintenanceConfig getMaintenanceConfig() {
        return maintenanceConfig;
    }

    public LoadProtectionConfig setMaintenanceConfig(MaintenanceConfig maintenanceConfig) {
        this.maintenanceConfig = Objects.requireNonNull(maintenanceConfig, "maintenanceConfig");
        return this;
    }

    public ConnectionBufferConfig getConnectionBufferConfig() {
        return connectionBufferConfig;
    }

    public LoadProtectionConfig setConnectionBufferConfig(ConnectionBufferConfig connectionBufferConfig) {
        this.connectionBufferConfig = Objects.requireNonNull(connectionBufferConfig, "connectionBufferConfig");
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LoadProtectionConfig that)) {
            return false;
        }
        return enabled == that.enabled
                && immutableMapNearCacheSize == that.immutableMapNearCacheSize
                && outboundConfig.equals(that.outboundConfig)
                && admissionConfig.equals(that.admissionConfig)
                && maintenanceConfig.equals(that.maintenanceConfig)
                && connectionBufferConfig.equals(that.connectionBufferConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled, immutableMapNearCacheSize, outboundConfig, admissionConfig, maintenanceConfig,
                connectionBufferConfig);
    }

    @Override
    public String toString() {
        return "LoadProtectionConfig{enabled=" + enabled
                + ", immutableMapNearCacheSize=" + immutableMapNearCacheSize
                + ", outboundConfig=" + outboundConfig
                + ", admissionConfig=" + admissionConfig
                + ", maintenanceConfig=" + maintenanceConfig
                + ", connectionBufferConfig=" + connectionBufferConfig
                + '}';
    }

    /**
     * Limits on the data a member queues for sending to clients. Bytes are counted twice: logical
     * bytes count every queued response (50,000 responses of a 1 MB value are 50 GB), physical
     * bytes count each shared value once (the same responses are 1 MB).
     */
    public static class OutboundConfig {

        /** Default for {@link #setMaxPendingBytesPerConnection(long)}: 64 MB. */
        public static final long DEFAULT_MAX_PENDING_BYTES_PER_CONNECTION = 64L << 20;
        /** Default for {@link #setMaxPhysicalPendingBytes(long)}: 1 GB. */
        public static final long DEFAULT_MAX_PHYSICAL_PENDING_BYTES = 1L << 30;
        /** Default for {@link #setSlowConsumerTimeoutSeconds(int)}. */
        public static final int DEFAULT_SLOW_CONSUMER_TIMEOUT_SECONDS = 30;

        private long maxPendingBytesPerConnection = DEFAULT_MAX_PENDING_BYTES_PER_CONNECTION;
        private long maxPhysicalPendingBytes = DEFAULT_MAX_PHYSICAL_PENDING_BYTES;
        private int slowConsumerTimeoutSeconds = DEFAULT_SLOW_CONSUMER_TIMEOUT_SECONDS;

        public OutboundConfig() {
        }

        public OutboundConfig(OutboundConfig config) {
            maxPendingBytesPerConnection = config.maxPendingBytesPerConnection;
            maxPhysicalPendingBytes = config.maxPhysicalPendingBytes;
            slowConsumerTimeoutSeconds = config.slowConsumerTimeoutSeconds;
        }

        /** Logical bytes queued for one client connection above which the member stops reading from it. */
        public long getMaxPendingBytesPerConnection() {
            return maxPendingBytesPerConnection;
        }

        public OutboundConfig setMaxPendingBytesPerConnection(long maxPendingBytesPerConnection) {
            this.maxPendingBytesPerConnection = checkPositive("maxPendingBytesPerConnection", maxPendingBytesPerConnection);
            return this;
        }

        /** Physical bytes queued for all client connections above which the member stops reading from clients. */
        public long getMaxPhysicalPendingBytes() {
            return maxPhysicalPendingBytes;
        }

        public OutboundConfig setMaxPhysicalPendingBytes(long maxPhysicalPendingBytes) {
            this.maxPhysicalPendingBytes = checkPositive("maxPhysicalPendingBytes", maxPhysicalPendingBytes);
            return this;
        }

        /** Seconds a client connection may have data queued without sending any before it is closed. */
        public int getSlowConsumerTimeoutSeconds() {
            return slowConsumerTimeoutSeconds;
        }

        public OutboundConfig setSlowConsumerTimeoutSeconds(int slowConsumerTimeoutSeconds) {
            this.slowConsumerTimeoutSeconds = checkPositive("slowConsumerTimeoutSeconds", slowConsumerTimeoutSeconds);
            return this;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof OutboundConfig that
                    && maxPendingBytesPerConnection == that.maxPendingBytesPerConnection
                    && maxPhysicalPendingBytes == that.maxPhysicalPendingBytes
                    && slowConsumerTimeoutSeconds == that.slowConsumerTimeoutSeconds;
        }

        @Override
        public int hashCode() {
            return Objects.hash(maxPendingBytesPerConnection, maxPhysicalPendingBytes, slowConsumerTimeoutSeconds);
        }

        @Override
        public String toString() {
            return "OutboundConfig{maxPendingBytesPerConnection=" + maxPendingBytesPerConnection
                    + ", maxPhysicalPendingBytes=" + maxPhysicalPendingBytes
                    + ", slowConsumerTimeoutSeconds=" + slowConsumerTimeoutSeconds + '}';
        }
    }

    /**
     * Limits on the client requests a member accepts. Above the per-connection limit the member
     * stops reading from the connection; above the member-wide limit, or with the heap above the
     * threshold after a garbage collection, it answers requests with
     * {@link uk.org.hecaton.core.HecatonOverloadException}.
     */
    public static class AdmissionConfig {

        /** Default for {@link #setMaxInFlightRequestsPerConnection(int)}. */
        public static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = 1_024;
        /** Default for {@link #setMaxInFlightRequests(int)}. */
        public static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 100_000;
        /** Default for {@link #setMaxMessageBytes(int)}: 64 MB. */
        public static final int DEFAULT_MAX_MESSAGE_BYTES = 64 << 20;
        /** Default for {@link #setHeapRejectThreshold(float)}. */
        public static final float DEFAULT_HEAP_REJECT_THRESHOLD = 0.9f;

        private int maxInFlightRequestsPerConnection = DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
        private int maxInFlightRequests = DEFAULT_MAX_IN_FLIGHT_REQUESTS;
        private int maxMessageBytes = DEFAULT_MAX_MESSAGE_BYTES;
        private float heapRejectThreshold = DEFAULT_HEAP_REJECT_THRESHOLD;

        public AdmissionConfig() {
        }

        public AdmissionConfig(AdmissionConfig config) {
            maxInFlightRequestsPerConnection = config.maxInFlightRequestsPerConnection;
            maxInFlightRequests = config.maxInFlightRequests;
            maxMessageBytes = config.maxMessageBytes;
            heapRejectThreshold = config.heapRejectThreshold;
        }

        /** Client requests of one connection being processed above which the member stops reading from it. */
        public int getMaxInFlightRequestsPerConnection() {
            return maxInFlightRequestsPerConnection;
        }

        public AdmissionConfig setMaxInFlightRequestsPerConnection(int maxInFlightRequestsPerConnection) {
            this.maxInFlightRequestsPerConnection = checkPositive("maxInFlightRequestsPerConnection",
                    maxInFlightRequestsPerConnection);
            return this;
        }

        /** Client requests being processed on the member above which new requests are refused. */
        public int getMaxInFlightRequests() {
            return maxInFlightRequests;
        }

        public AdmissionConfig setMaxInFlightRequests(int maxInFlightRequests) {
            this.maxInFlightRequests = checkPositive("maxInFlightRequests", maxInFlightRequests);
            return this;
        }

        /** The largest client message the member reads, from authenticated clients too. */
        public int getMaxMessageBytes() {
            return maxMessageBytes;
        }

        public AdmissionConfig setMaxMessageBytes(int maxMessageBytes) {
            this.maxMessageBytes = checkPositive("maxMessageBytes", maxMessageBytes);
            return this;
        }

        /**
         * Fraction of the maximum heap that may be used after a garbage collection before the
         * member refuses client requests; {@code 0} turns the check off.
         */
        public float getHeapRejectThreshold() {
            return heapRejectThreshold;
        }

        public AdmissionConfig setHeapRejectThreshold(float heapRejectThreshold) {
            if (heapRejectThreshold < 0 || heapRejectThreshold > 1) {
                throw new IllegalArgumentException("heapRejectThreshold must be between 0 and 1, was " + heapRejectThreshold);
            }
            this.heapRejectThreshold = heapRejectThreshold;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof AdmissionConfig that
                    && maxInFlightRequestsPerConnection == that.maxInFlightRequestsPerConnection
                    && maxInFlightRequests == that.maxInFlightRequests
                    && maxMessageBytes == that.maxMessageBytes
                    && Float.compare(heapRejectThreshold, that.heapRejectThreshold) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(maxInFlightRequestsPerConnection, maxInFlightRequests, maxMessageBytes, heapRejectThreshold);
        }

        @Override
        public String toString() {
            return "AdmissionConfig{maxInFlightRequestsPerConnection=" + maxInFlightRequestsPerConnection
                    + ", maxInFlightRequests=" + maxInFlightRequests
                    + ", maxMessageBytes=" + maxMessageBytes
                    + ", heapRejectThreshold=" + heapRejectThreshold + '}';
        }
    }

    /**
     * Threads and limits for connection lifecycle work: the cleanup after client disconnects runs on
     * its own threads and in batches, and the member admits new client connections and
     * authentications at a bounded rate.
     */
    public static class MaintenanceConfig {

        /** Default for {@link #setThreadCount(int)}. */
        public static final int DEFAULT_THREAD_COUNT = 2;
        /** Default for {@link #setMaxBacklog(int)}. */
        public static final int DEFAULT_MAX_BACKLOG = 100_000;
        /** Default for {@link #setMaxAcceptsPerSecond(int)}: no limit. */
        public static final int DEFAULT_MAX_ACCEPTS_PER_SECOND = 0;
        /** Default for {@link #setMaxConcurrentAuthentications(int)}. */
        public static final int DEFAULT_MAX_CONCURRENT_AUTHENTICATIONS = 256;

        private int threadCount = DEFAULT_THREAD_COUNT;
        private int maxBacklog = DEFAULT_MAX_BACKLOG;
        private int maxAcceptsPerSecond = DEFAULT_MAX_ACCEPTS_PER_SECOND;
        private int maxConcurrentAuthentications = DEFAULT_MAX_CONCURRENT_AUTHENTICATIONS;

        public MaintenanceConfig() {
        }

        public MaintenanceConfig(MaintenanceConfig config) {
            threadCount = config.threadCount;
            maxBacklog = config.maxBacklog;
            maxAcceptsPerSecond = config.maxAcceptsPerSecond;
            maxConcurrentAuthentications = config.maxConcurrentAuthentications;
        }

        /** Threads that run client disconnect cleanup. */
        public int getThreadCount() {
            return threadCount;
        }

        public MaintenanceConfig setThreadCount(int threadCount) {
            this.threadCount = checkPositive("threadCount", threadCount);
            return this;
        }

        /** Disconnected clients waiting for cleanup above which the member refuses new client connections. */
        public int getMaxBacklog() {
            return maxBacklog;
        }

        public MaintenanceConfig setMaxBacklog(int maxBacklog) {
            this.maxBacklog = checkPositive("maxBacklog", maxBacklog);
            return this;
        }

        /** New client connections the member accepts per second; {@code 0} for no limit. */
        public int getMaxAcceptsPerSecond() {
            return maxAcceptsPerSecond;
        }

        public MaintenanceConfig setMaxAcceptsPerSecond(int maxAcceptsPerSecond) {
            this.maxAcceptsPerSecond = checkNotNegative(maxAcceptsPerSecond, "maxAcceptsPerSecond must not be negative");
            return this;
        }

        /** Client authentications in progress above which further client connections are refused. */
        public int getMaxConcurrentAuthentications() {
            return maxConcurrentAuthentications;
        }

        public MaintenanceConfig setMaxConcurrentAuthentications(int maxConcurrentAuthentications) {
            this.maxConcurrentAuthentications = checkPositive("maxConcurrentAuthentications", maxConcurrentAuthentications);
            return this;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof MaintenanceConfig that
                    && threadCount == that.threadCount
                    && maxBacklog == that.maxBacklog
                    && maxAcceptsPerSecond == that.maxAcceptsPerSecond
                    && maxConcurrentAuthentications == that.maxConcurrentAuthentications;
        }

        @Override
        public int hashCode() {
            return Objects.hash(threadCount, maxBacklog, maxAcceptsPerSecond, maxConcurrentAuthentications);
        }

        @Override
        public String toString() {
            return "MaintenanceConfig{threadCount=" + threadCount
                    + ", maxBacklog=" + maxBacklog
                    + ", maxAcceptsPerSecond=" + maxAcceptsPerSecond
                    + ", maxConcurrentAuthentications=" + maxConcurrentAuthentications + '}';
        }
    }

    /**
     * The socket-side buffers of client connections. Each client connection otherwise allocates
     * its read and write buffers (and, with TLS, two more) when it is opened, at the socket buffer
     * size (128 KB by default), whether or not it ever sends anything. With pooling, connections
     * take buffers from a member-wide pool while they read or write and give them back when idle.
     * The kernel's socket buffers ({@code SO_RCVBUF}, {@code SO_SNDBUF}) keep their configured size:
     * TCP can stall when they are smaller than a segment (64 KB on loopback).
     */
    public static class ConnectionBufferConfig {

        /** Default for {@link #setClientBufferBytes(int)}: 32 KB. */
        public static final int DEFAULT_CLIENT_BUFFER_BYTES = 32 << 10;
        /** Default for {@link #setMaxPooledBytes(long)}: 1 GB. */
        public static final long DEFAULT_MAX_POOLED_BYTES = 1L << 30;

        private boolean pooled = true;
        private int clientBufferBytes = DEFAULT_CLIENT_BUFFER_BYTES;
        private long maxPooledBytes = DEFAULT_MAX_POOLED_BYTES;

        public ConnectionBufferConfig() {
        }

        public ConnectionBufferConfig(ConnectionBufferConfig config) {
            pooled = config.pooled;
            clientBufferBytes = config.clientBufferBytes;
            maxPooledBytes = config.maxPooledBytes;
        }

        /** Whether client connections borrow their buffers from a pool while busy. */
        public boolean isPooled() {
            return pooled;
        }

        public ConnectionBufferConfig setPooled(boolean pooled) {
            this.pooled = pooled;
            return this;
        }

        /**
         * The size of the client protocol's read and write buffers of a client connection, in the
         * heap; not the kernel's socket buffers.
         */
        public int getClientBufferBytes() {
            return clientBufferBytes;
        }

        public ConnectionBufferConfig setClientBufferBytes(int clientBufferBytes) {
            this.clientBufferBytes = checkPositive("clientBufferBytes", clientBufferBytes);
            return this;
        }

        /** Bytes of idle buffers the pool keeps for reuse; a connection that finds the pool empty allocates one. */
        public long getMaxPooledBytes() {
            return maxPooledBytes;
        }

        public ConnectionBufferConfig setMaxPooledBytes(long maxPooledBytes) {
            this.maxPooledBytes = checkPositive("maxPooledBytes", maxPooledBytes);
            return this;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof ConnectionBufferConfig that
                    && pooled == that.pooled
                    && clientBufferBytes == that.clientBufferBytes
                    && maxPooledBytes == that.maxPooledBytes;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pooled, clientBufferBytes, maxPooledBytes);
        }

        @Override
        public String toString() {
            return "ConnectionBufferConfig{pooled=" + pooled
                    + ", clientBufferBytes=" + clientBufferBytes
                    + ", maxPooledBytes=" + maxPooledBytes + '}';
        }
    }
}
