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

package uk.org.hecaton.internal.diagnostics;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.cluster.MembershipAdapter;
import uk.org.hecaton.cluster.MembershipEvent;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.LifecycleEvent;
import uk.org.hecaton.core.LifecycleListener;
import uk.org.hecaton.instance.impl.NodeExtension;
import uk.org.hecaton.internal.cluster.ClusterVersionListener;
import uk.org.hecaton.internal.nio.Connection;
import uk.org.hecaton.internal.nio.ConnectionListenable;
import uk.org.hecaton.internal.nio.ConnectionListener;
import uk.org.hecaton.internal.server.ServerConnection;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.partition.MigrationListener;
import uk.org.hecaton.partition.MigrationState;
import uk.org.hecaton.partition.ReplicaMigrationEvent;
import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.spi.properties.HecatonProperty;
import uk.org.hecaton.version.Version;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The {@link SystemLogPlugin} is responsible for:
 * <ol>
 * <li>Showing lifecycle changes like shutting down, merging etc.</li>
 * <li>Show connection creation and connection closing. Also the causes of closes are included.</li>
 * <li>Showing membership changes.</li>
 * <li>Optionally showing partition migration</li>
 * </ol>
 * This plugin is very useful to get an idea what is happening inside a cluster;
 * especially when there are connection related problems.
 * <p>
 * This plugin has a low overhead and is meant to run in production.
 */
public class SystemLogPlugin extends DiagnosticsPlugin {

    /**
     * If this plugin is enabled.
     */
    public static final HecatonProperty ENABLED
            = new HecatonProperty("hecaton.diagnostics.systemlog.enabled", "true");

    /**
     * If logging partition migration is enabled. Because there can be so many partitions, logging the partition migration
     * can be very noisy.
     */
    public static final HecatonProperty LOG_PARTITIONS
            = new HecatonProperty("hecaton.diagnostics.systemlog.partitions", "false");

    /**
     * Currently, the Diagnostic is scheduler-based, so each task runs as frequently as it has been configured. This works
     * fine for most plugins, but if there are outside events triggering the need to write, this scheduler based model is
     * not the right fit. In the future the Diagnostics need to be improved.
     */
    private static final long PERIOD_MILLIS = SECONDS.toMillis(1);

    private final Queue<Object> logQueue = new ConcurrentLinkedQueue<>();
    private final ConnectionListenable connectionObservable;
    private final HecatonInstance hecatonInstance;
    private final Address thisAddress;
    private final NodeExtension nodeExtension;
    private final HecatonProperties properties;
    private boolean logPartitions;
    private volatile boolean enabled;
    private ClusterVersionListenerImpl clusterVersionListener;
    private ConnectionListenerImpl connectionListener;
    private MembershipListenerImpl membershipListener;
    private MigrationListenerImpl migrationListener;
    private LifecycleListenerImpl lifecycleListener;

    public SystemLogPlugin(HecatonProperties properties,
                           ConnectionListenable connectionObservable,
                           HecatonInstance hecatonInstance,
                           ILogger logger) {
        this(logger, properties, connectionObservable, hecatonInstance, null);
    }

    public SystemLogPlugin(ILogger logger,
                           HecatonProperties properties,
                           ConnectionListenable connectionObservable,
                           HecatonInstance hecatonInstance,
                           NodeExtension nodeExtension) {
        super(logger);
        this.connectionObservable = connectionObservable;
        this.hecatonInstance = hecatonInstance;
        this.thisAddress = getThisAddress(hecatonInstance);
        this.nodeExtension = nodeExtension;
        this.properties = properties;
        readProperties();
    }

    @Override
    void readProperties() {
        this.logPartitions = properties.getBoolean(overrideProperty(LOG_PARTITIONS));
        this.enabled = properties.getBoolean(overrideProperty(ENABLED));
    }

    private Address getThisAddress(HecatonInstance hecatonInstance) {
        try {
            return hecatonInstance.getCluster().getLocalMember().getAddress();
        } catch (UnsupportedOperationException e) {
            return null;
        }
    }

    @Override
    public long getPeriodMillis() {
        if (!enabled) {
            return DiagnosticsPlugin.NOT_SCHEDULED_PERIOD_MS;
        }
        return PERIOD_MILLIS;
    }

    @Override
    public void onStart() {
        super.onStart();
        logger.info("Plugin:active: logPartitions:" + logPartitions);

        // the listeners should be registered once during the lifetime of the Diagnostics service which is tied to
        // Hecaton instance
        if (connectionListener == null) {
            connectionListener = new ConnectionListenerImpl();
            connectionObservable.addConnectionListener(connectionListener);
        }

        if (membershipListener == null) {
            membershipListener = new MembershipListenerImpl();
            hecatonInstance.getCluster().addMembershipListener(membershipListener);
        }

        if (logPartitions) {
            if (migrationListener == null) {
                migrationListener = new MigrationListenerImpl();
                hecatonInstance.getPartitionService().addMigrationListener(migrationListener);
            }
        }

        if (lifecycleListener == null) {
            lifecycleListener = new LifecycleListenerImpl();
            hecatonInstance.getLifecycleService().addLifecycleListener(lifecycleListener);
        }

        if (nodeExtension != null) {
            if (clusterVersionListener == null) {
                clusterVersionListener = new ClusterVersionListenerImpl();
                nodeExtension.registerListener(clusterVersionListener);
            }
        }
    }

    @Override
    public void onShutdown() {
        super.onShutdown();
        logQueue.clear();
        logger.info("Plugin:inactive");
    }

    @Override
    public void run(DiagnosticsLogWriter writer) {
        for (; ; ) {
            Object item = logQueue.poll();
            if (item == null || !isActive()) {
                return;
            }

            if (item instanceof LifecycleEvent event) {
                render(writer, event);
            } else if (item instanceof MembershipEvent event) {
                render(writer, event);
            } else if (item instanceof MigrationState state) {
                render(writer, state);
            } else if (item instanceof ReplicaMigrationEvent event) {
                render(writer, event);
            } else if (item instanceof ConnectionEvent event) {
                render(writer, event);
            } else if (item instanceof Version version) {
                render(writer, version);
            }
        }
    }

    // The plugins are there to be used by the diagnostics service always. If the service disabled at runtime,
    // the plugin stays, and so its listeners since there is no deregister method for listeners.
    // To avoid the listeners to be called when the plugin is inactive.
    private boolean shouldListenersListen() {
        return enabled && isActive();
    }

    //for testing
    boolean getLogPartitions() {
        return logPartitions;
    }

    private void render(DiagnosticsLogWriter writer, LifecycleEvent event) {
        writer.startSection("Lifecycle");
        writer.writeEntry(event.getState().name());
        writer.endSection();
    }

    private void render(DiagnosticsLogWriter writer, MembershipEvent event) {
        switch (event.getEventType()) {
            case MembershipEvent.MEMBER_ADDED -> writer.startSection("MemberAdded");
            case MembershipEvent.MEMBER_REMOVED -> writer.startSection("MemberRemoved");
            default -> {
                return;
            }
        }
        writer.writeKeyValueEntry("member", event.getMember().getAddress().toString());

        // writing the members
        writer.startSection("Members");
        Set<Member> members = event.getMembers();
        if (members != null) {
            boolean first = true;
            for (Member member : members) {
                Address memberAddress = member.getAddress();
                String addressStr = String.valueOf(memberAddress);
                if (memberAddress.equals(thisAddress)) {
                    if (first) {
                        writer.writeEntry(addressStr + ":this:master");
                    } else {
                        writer.writeEntry(addressStr + ":this");
                    }
                } else if (first) {
                    writer.writeEntry(addressStr + ":master");
                } else {
                    writer.writeEntry(addressStr);
                }
                first = false;
            }
        }
        writer.endSection();

        // ending the outer section
        writer.endSection();
    }

    private void render(DiagnosticsLogWriter writer, MigrationState migrationState) {
        writer.startSection("MigrationState");
        writer.writeKeyValueEntryAsDateTime("startTime", migrationState.getStartTime());
        writer.writeKeyValueEntry("plannedMigrations", migrationState.getPlannedMigrations());
        writer.writeKeyValueEntry("completedMigrations", migrationState.getCompletedMigrations());
        writer.writeKeyValueEntry("remainingMigrations", migrationState.getRemainingMigrations());
        writer.writeKeyValueEntry("totalElapsedTime(ms)", migrationState.getTotalElapsedTime());
        writer.endSection();
    }

    private void render(DiagnosticsLogWriter writer, ReplicaMigrationEvent event) {
        if (event.isSuccess()) {
            writer.startSection("MigrationCompleted");
        } else {
            writer.startSection("MigrationFailed");
        }

        Member source = event.getSource();
        writer.writeKeyValueEntry("source", source == null ? "null" : source.getAddress().toString());
        writer.writeKeyValueEntry("destination", event.getDestination().getAddress().toString());
        writer.writeKeyValueEntry("partitionId", event.getPartitionId());
        writer.writeKeyValueEntry("replicaIndex", event.getReplicaIndex());
        writer.writeKeyValueEntry("elapsedTime(ms)", event.getReplicaIndex());

        render(writer, event.getMigrationState());
        writer.endSection();
    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void render(DiagnosticsLogWriter writer, ConnectionEvent event) {
        if (event.added) {
            writer.startSection("ConnectionAdded");
        } else {
            writer.startSection("ConnectionRemoved");
        }

        Connection connection = event.connection;
        writer.writeEntry(connection.toString());

        if (connection instanceof ServerConnection serverConnection) {
            writer.writeKeyValueEntry("type", serverConnection.getConnectionType());
        }
        writer.writeKeyValueEntry("isAlive", connection.isAlive());

        if (!event.added) {
            String closeReason = connection.getCloseReason();
            Throwable closeCause = connection.getCloseCause();
            if (closeReason == null && closeCause != null) {
                closeReason = closeCause.getMessage();
            }

            writer.writeKeyValueEntry("closeReason", closeReason == null ? "Unknown" : closeReason);

            if (closeCause != null) {
                writer.startSection("CloseCause");
                String s = closeCause.getClass().getName();
                String message = closeCause.getMessage();
                writer.writeEntry((message != null) ? (s + ": " + message) : s);

                for (StackTraceElement element : closeCause.getStackTrace()) {
                    writer.writeEntry(element.toString());
                }
                writer.endSection();
            }
        }
        writer.endSection();
    }

    private void render(DiagnosticsLogWriter writer, Version version) {
        writer.startSection("ClusterVersionChanged");
        writer.writeEntry(version.toString());
        writer.endSection();
    }

    protected class LifecycleListenerImpl implements LifecycleListener {
        @Override
        public void stateChanged(LifecycleEvent event) {
            logQueue.add(event);
        }
    }

    private static final class ConnectionEvent {
        final boolean added;
        final Connection connection;

        ConnectionEvent(boolean added, Connection connection) {
            this.added = added;
            this.connection = connection;
        }
    }

    protected class ConnectionListenerImpl implements ConnectionListener {
        @Override
        public void connectionAdded(Connection connection) {
            if (shouldListenersListen()) {
                logQueue.add(new ConnectionEvent(true, connection));
            }
        }

        @Override
        public void connectionRemoved(Connection connection) {
            if (shouldListenersListen()) {
                logQueue.add(new ConnectionEvent(false, connection));
            }
        }
    }

    protected class MembershipListenerImpl extends MembershipAdapter {
        @Override
        public void memberAdded(MembershipEvent event) {
            if (shouldListenersListen()) {
                logQueue.add(event);
            }
        }

        @Override
        public void memberRemoved(MembershipEvent event) {
            if (shouldListenersListen()) {
                logQueue.add(event);
            }
        }
    }

    protected class MigrationListenerImpl implements MigrationListener {
        @Override
        public void migrationStarted(MigrationState state) {
            if (shouldListenersListen()) {
                logQueue.add(state);
            }
        }

        @Override
        public void migrationFinished(MigrationState state) {
            if (shouldListenersListen()) {
                logQueue.add(state);
            }
        }

        @Override
        public void replicaMigrationCompleted(ReplicaMigrationEvent event) {
            if (shouldListenersListen()) {
                logQueue.add(event);
            }
        }

        @Override
        public void replicaMigrationFailed(ReplicaMigrationEvent event) {
            if (shouldListenersListen()) {
                logQueue.add(event);
            }
        }
    }

    protected class ClusterVersionListenerImpl implements ClusterVersionListener {
        @Override
        public void onClusterVersionChange(Version newVersion) {
            if (shouldListenersListen()) {
                logQueue.add(newVersion);
            }
        }
    }
}
