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

package uk.org.hecaton.test.compatibility;

import uk.org.hecaton.auditlog.AuditlogService;
import uk.org.hecaton.auditlog.impl.NoOpAuditlogService;
import uk.org.hecaton.client.impl.ClientEngine;
import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.config.SSLConfig;
import uk.org.hecaton.cp.CPSubsystem;
import uk.org.hecaton.cp.internal.persistence.CPPersistenceService;
import uk.org.hecaton.hotrestart.HotRestartService;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.instance.impl.HecatonInstanceImpl;
import uk.org.hecaton.instance.impl.NodeExtension;
import uk.org.hecaton.internal.ascii.TextCommandService;
import uk.org.hecaton.internal.cluster.impl.JoinMessage;
import uk.org.hecaton.internal.diagnostics.Diagnostics;
import uk.org.hecaton.internal.diagnostics.HealthMonitor;
import uk.org.hecaton.internal.hotrestart.InternalHotRestartService;
import uk.org.hecaton.internal.jmx.ManagementService;
import uk.org.hecaton.internal.management.TimedMemberStateFactory;
import uk.org.hecaton.internal.memory.MemoryStats;
import uk.org.hecaton.internal.namespace.UserCodeNamespaceService;
import uk.org.hecaton.internal.networking.ChannelInitializer;
import uk.org.hecaton.internal.networking.InboundHandler;
import uk.org.hecaton.internal.networking.OutboundHandler;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.impl.AbstractSerializationService;
import uk.org.hecaton.internal.serialization.impl.compact.schema.MemberSchemaService;
import uk.org.hecaton.internal.server.ServerConnection;
import uk.org.hecaton.internal.server.ServerContext;
import uk.org.hecaton.internal.tpc.TpcServerBootstrap;
import uk.org.hecaton.internal.tpc.TpcServerBootstrapImpl;
import uk.org.hecaton.internal.util.ByteArrayProcessor;
import uk.org.hecaton.jet.JetService;
import uk.org.hecaton.jet.impl.JetServiceBackend;
import uk.org.hecaton.nio.MemberSocketInterceptor;
import uk.org.hecaton.nio.ssl.SSLEngineFactory;
import uk.org.hecaton.security.SecurityService;
import uk.org.hecaton.security.impl.InternalSecurityContext;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.version.Version;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

/**
 * Node extension that instantiates a {@link SamplingSerializationService} when asked to create
 * {@link SerializationService} instance.
 */
public class SamplingNodeExtension implements NodeExtension {

    private final NodeExtension nodeExtension;

    public SamplingNodeExtension(NodeExtension nodeExtension) {
        this.nodeExtension = nodeExtension;
    }

    @Override
    public InternalSerializationService createSerializationService() {
        InternalSerializationService serializationService = nodeExtension.createSerializationService();
        return new SamplingSerializationService((AbstractSerializationService) serializationService);
    }

    @Override
    public InternalSerializationService createCompatibilitySerializationService() {
        InternalSerializationService serializationService = nodeExtension.createCompatibilitySerializationService();
        return new SamplingSerializationService((AbstractSerializationService) serializationService);
    }

    @Override
    public MemberSchemaService createSchemaService() {
        return nodeExtension.createSchemaService();
    }

    @Override
    public SecurityService getSecurityService() {
        return nodeExtension.getSecurityService();
    }

    @Override
    public void beforeStart() {
        nodeExtension.beforeStart();
    }

    @Override
    public void printNodeInfo() {
        nodeExtension.printNodeInfo();
    }

    @Override
    public void logInstanceTrackingMetadata() {
        nodeExtension.logInstanceTrackingMetadata();
    }

    @Override
    public void beforeJoin() {
        nodeExtension.beforeJoin();
    }

    @Override
    public void afterStart() {
        nodeExtension.afterStart();
    }

    @Override
    public boolean isStartCompleted() {
        return nodeExtension.isStartCompleted();
    }

    @Override
    public boolean isReady() {
        return nodeExtension.isReady();
    }

    @Override
    public void beforeShutdown(boolean terminate) {
        nodeExtension.beforeShutdown(terminate);
    }

    @Override
    public void shutdown() {
        nodeExtension.shutdown();
    }

    @Override
    public void afterShutdown() {
        nodeExtension.afterShutdown();
    }

    @Override
    public InternalSecurityContext getSecurityContext() {
        return nodeExtension.getSecurityContext();
    }

    @Nullable
    @Override
    public InternalSecurityContext getSecurityContextOrNull() {
        return nodeExtension.getSecurityContextOrNull();
    }

    @Override
    public <T> T createService(Class<T> type, Object... params) {
        return nodeExtension.createService(type, params);
    }

    @Override
    public Map<String, Object> createExtensionServices() {
        return nodeExtension.createExtensionServices();
    }

    @Override
    public MemberSocketInterceptor getSocketInterceptor(EndpointQualifier endpointQualifier) {
        return nodeExtension.getSocketInterceptor(endpointQualifier);
    }

    @Override
    public InboundHandler[] createInboundHandlers(EndpointQualifier qualifier, ServerConnection connection, ServerContext serverContext) {
        return nodeExtension.createInboundHandlers(qualifier, connection, serverContext);
    }

    @Override
    public OutboundHandler[] createOutboundHandlers(EndpointQualifier qualifier, ServerConnection connection, ServerContext serverContext) {
        return nodeExtension.createOutboundHandlers(qualifier, connection, serverContext);
    }

    @Override
    public Function<EndpointQualifier, ChannelInitializer> createChannelInitializerFn(ServerContext serverContext) {
       return nodeExtension.createChannelInitializerFn(serverContext);
    }

    @Override
    public void onThreadStart(Thread thread) {
        nodeExtension.onThreadStart(thread);
    }

    @Override
    public void onThreadStop(Thread thread) {
        nodeExtension.onThreadStop(thread);
    }

    @Override
    public MemoryStats getMemoryStats() {
        return nodeExtension.getMemoryStats();
    }

    @Override
    public void validateJoinRequest(JoinMessage joinMessage) {
        nodeExtension.validateJoinRequest(joinMessage);
    }

    @Override
    public Set<Version> getSupportedVersions() {
        return nodeExtension.getSupportedVersions();
    }

    @Override
    public void beforeClusterStateChange(ClusterState currState, ClusterState requestedState, boolean isTransient) {
        nodeExtension.beforeClusterStateChange(currState, requestedState, isTransient);
    }

    @Override
    public void onInitialClusterState(ClusterState initialState) {
        nodeExtension.onInitialClusterState(initialState);
    }

    @Override
    public void onClusterStateChange(ClusterState newState, boolean isTransient) {
        nodeExtension.onClusterStateChange(newState, isTransient);
    }

    @Override
    public void afterClusterStateChange(ClusterState oldState, ClusterState newState, boolean isTransient) {
        nodeExtension.afterClusterStateChange(oldState, newState, isTransient);
    }

    @Override
    public void onPartitionStateChange() {
        nodeExtension.onPartitionStateChange();
    }

    @Override
    public void onMemberListChange() {
        nodeExtension.onMemberListChange();
    }

    @Override
    public void onClusterVersionChange(Version newVersion) {
        nodeExtension.onClusterVersionChange(newVersion);
    }

    @Override
    public boolean isNodeVersionCompatibleWith(Version clusterVersion) {
        return nodeExtension.isNodeVersionCompatibleWith(clusterVersion);
    }

    @Override
    public boolean registerListener(Object listener) {
        return nodeExtension.registerListener(listener);
    }

    @Override
    public HotRestartService getHotRestartService() {
        return nodeExtension.getHotRestartService();
    }

    @Override
    public InternalHotRestartService getInternalHotRestartService() {
        return nodeExtension.getInternalHotRestartService();
    }

    @Override
    public UUID createMemberUuid() {
        return nodeExtension.createMemberUuid();
    }

    @Override
    public TimedMemberStateFactory createTimedMemberStateFactory(HecatonInstanceImpl instance) {
        return nodeExtension.createTimedMemberStateFactory(instance);
    }

    @Override
    public ByteArrayProcessor createMulticastInputProcessor(ServerContext serverContext) {
        return nodeExtension.createMulticastInputProcessor(serverContext);
    }

    @Override
    public ByteArrayProcessor createMulticastOutputProcessor(ServerContext serverContext) {
        return nodeExtension.createMulticastOutputProcessor(serverContext);
    }

    @Override
    public void registerPlugins(Diagnostics diagnostics) {
    }

    @Override
    public ManagementService createJMXManagementService(HecatonInstanceImpl instance) {
        return nodeExtension.createJMXManagementService(instance);
    }

    @Override
    public TextCommandService createTextCommandService() {
        return nodeExtension.createTextCommandService();
    }

    @Override
    public void scheduleClusterVersionAutoUpgrade() {
        nodeExtension.scheduleClusterVersionAutoUpgrade();
    }

    @Override
    public boolean isClientFailoverSupported() {
        return false;
    }

    @Override
    public AuditlogService getAuditlogService() {
        return NoOpAuditlogService.INSTANCE;
    }

    @Override
    public CPPersistenceService getCPPersistenceService() {
        return nodeExtension.getCPPersistenceService();
    }

    @Override
    public CPSubsystem createCPSubsystem(NodeEngine nodeEngine) {
        return nodeExtension.createCPSubsystem(nodeEngine);
    }

    @Override
    public JetService getJet() {
        return nodeExtension.getJet();
    }

    @Nullable
    @Override
    public JetServiceBackend getJetServiceBackend() {
        return nodeExtension.getJetServiceBackend();
    }

    @Override
    public SSLEngineFactory createSslEngineFactory(SSLConfig sslConfig) {
        return nodeExtension.createSslEngineFactory(sslConfig);
    }

    @Override
    public UserCodeNamespaceService getNamespaceService() {
        return nodeExtension.getNamespaceService();
    }

    @Override
    public TpcServerBootstrap createTpcServerBootstrap() {
        return new TpcServerBootstrapImpl(null);
    }

    @Override
    public ClientEngine createClientEngine() {
        return nodeExtension.createClientEngine();
    }

    @Nullable
    @Override
    public Object getLicense() {
        return nodeExtension.getLicense();
    }

    @Override
    public HealthMonitor createHealthMonitor() {
        return nodeExtension.createHealthMonitor();
    }
}
