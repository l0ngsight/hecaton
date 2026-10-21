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

package uk.org.hecaton.instance.impl;

import uk.org.hecaton.auditlog.AuditlogService;
import uk.org.hecaton.auditlog.AuditlogServiceFactory;
import uk.org.hecaton.auditlog.impl.ILoggerAuditlogServiceFactory;
import uk.org.hecaton.auditlog.impl.NoOpAuditlogService;
import uk.org.hecaton.cache.impl.CacheService;
import uk.org.hecaton.cache.impl.ICacheService;
import uk.org.hecaton.client.impl.ClientEngine;
import uk.org.hecaton.client.impl.ClientEngineImpl;
import uk.org.hecaton.client.impl.ClusterViewListenerService;
import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.AdvancedNetworkConfig;
import uk.org.hecaton.config.AuditlogConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.EndpointConfig;
import uk.org.hecaton.config.InstanceTrackingConfig;
import uk.org.hecaton.config.InstanceTrackingConfig.InstanceMode;
import uk.org.hecaton.config.InstanceTrackingConfig.InstanceProductName;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.PersistenceConfig;
import uk.org.hecaton.config.SSLConfig;
import uk.org.hecaton.config.SerializationConfig;
import uk.org.hecaton.config.SocketInterceptorConfig;
import uk.org.hecaton.config.SymmetricEncryptionConfig;
import uk.org.hecaton.config.cp.CPSubsystemConfig;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.cp.CPSubsystem;
import uk.org.hecaton.cp.internal.CPSubsystemImpl;
import uk.org.hecaton.cp.internal.persistence.CPPersistenceService;
import uk.org.hecaton.cp.internal.persistence.CPPersistenceServiceImpl;
import uk.org.hecaton.cp.internal.persistence.NopCPPersistenceService;
import uk.org.hecaton.hotrestart.HotRestartService;
import uk.org.hecaton.instance.BuildInfo;
import uk.org.hecaton.instance.BuildInfoProvider;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.instance.module.MemberModules;
import uk.org.hecaton.instance.module.NativeMemoryCapability;
import uk.org.hecaton.instance.module.PersistenceCapability;
import uk.org.hecaton.internal.ascii.TextCommandService;
import uk.org.hecaton.internal.ascii.TextCommandServiceImpl;
import uk.org.hecaton.internal.cluster.ClusterStateListener;
import uk.org.hecaton.internal.cluster.ClusterVersionListener;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.internal.cluster.impl.JoinMessage;
import uk.org.hecaton.internal.cluster.impl.JoinRequest;
import uk.org.hecaton.internal.cluster.impl.SplitBrainJoinMessage;
import uk.org.hecaton.internal.cluster.impl.VersionMismatchException;
import uk.org.hecaton.internal.diagnostics.BuildInfoPlugin;
import uk.org.hecaton.internal.diagnostics.ConfigPropertiesPlugin;
import uk.org.hecaton.internal.diagnostics.Diagnostics;
import uk.org.hecaton.internal.diagnostics.EventQueuePlugin;
import uk.org.hecaton.internal.diagnostics.HealthMonitor;
import uk.org.hecaton.internal.diagnostics.InvocationProfilerPlugin;
import uk.org.hecaton.internal.diagnostics.InvocationSamplePlugin;
import uk.org.hecaton.internal.diagnostics.MemberHecatonInstanceInfoPlugin;
import uk.org.hecaton.internal.diagnostics.MemberHeartbeatPlugin;
import uk.org.hecaton.internal.diagnostics.MetricsPlugin;
import uk.org.hecaton.internal.diagnostics.NetworkingImbalancePlugin;
import uk.org.hecaton.internal.diagnostics.OperationHeartbeatPlugin;
import uk.org.hecaton.internal.diagnostics.OperationProfilerPlugin;
import uk.org.hecaton.internal.diagnostics.OperationThreadSamplerPlugin;
import uk.org.hecaton.internal.diagnostics.OverloadedConnectionsPlugin;
import uk.org.hecaton.internal.diagnostics.PendingInvocationsPlugin;
import uk.org.hecaton.internal.diagnostics.SlowOperationPlugin;
import uk.org.hecaton.internal.diagnostics.StoreLatencyPlugin;
import uk.org.hecaton.internal.diagnostics.SystemLogPlugin;
import uk.org.hecaton.internal.diagnostics.SystemPropertiesPlugin;
import uk.org.hecaton.internal.dynamicconfig.ClusterWideConfigurationService;
import uk.org.hecaton.internal.dynamicconfig.EmptyDynamicConfigListener;
import uk.org.hecaton.internal.hotrestart.InternalHotRestartService;
import uk.org.hecaton.internal.hotrestart.NoOpHotRestartService;
import uk.org.hecaton.internal.hotrestart.NoopInternalHotRestartService;
import uk.org.hecaton.internal.jmx.ManagementService;
import uk.org.hecaton.internal.management.TimedMemberStateFactory;
import uk.org.hecaton.internal.memory.DefaultMemoryStats;
import uk.org.hecaton.internal.memory.MemoryStats;
import uk.org.hecaton.internal.namespace.UserCodeNamespaceService;
import uk.org.hecaton.internal.namespace.impl.NoOpUserCodeNamespaceService;
import uk.org.hecaton.internal.namespace.impl.UserCodeNamespaceServiceImpl;
import uk.org.hecaton.internal.namespace.impl.NodeEngineThreadLocalContext;
import uk.org.hecaton.internal.networking.ChannelInitializer;
import uk.org.hecaton.internal.networking.InboundHandler;
import uk.org.hecaton.internal.networking.OutboundHandler;
import uk.org.hecaton.internal.nio.ClassLoaderUtil;
import uk.org.hecaton.internal.nio.ssl.SSLEngineFactories;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.SerializationServiceBuilder;
import uk.org.hecaton.internal.serialization.impl.CodebaseClusterVersionAware;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.internal.serialization.impl.compact.schema.MemberSchemaService;
import uk.org.hecaton.internal.server.ServerConnection;
import uk.org.hecaton.internal.server.ServerContext;
import uk.org.hecaton.internal.server.tcp.ChannelInitializerFunction;
import uk.org.hecaton.internal.server.tcp.PacketDecoder;
import uk.org.hecaton.internal.server.tcp.PacketEncoder;
import uk.org.hecaton.internal.tpc.TpcServerBootstrap;
import uk.org.hecaton.internal.tpc.TpcServerBootstrapImpl;
import uk.org.hecaton.internal.util.ConstructorFunction;
import uk.org.hecaton.internal.util.HecatonFeature;
import uk.org.hecaton.internal.util.JVMUtil;
import uk.org.hecaton.internal.util.MapUtil;
import uk.org.hecaton.internal.util.Preconditions;
import uk.org.hecaton.jet.JetService;
import uk.org.hecaton.jet.config.JetConfig;
import uk.org.hecaton.jet.impl.JetServiceBackend;
import uk.org.hecaton.jet.impl.JobEventService;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.nio.MemberSocketInterceptor;
import uk.org.hecaton.nio.ssl.SSLEngineFactory;
import uk.org.hecaton.partition.PartitioningStrategy;
import uk.org.hecaton.partition.strategy.DefaultPartitioningStrategy;
import uk.org.hecaton.security.SecurityService;
import uk.org.hecaton.security.impl.InternalSecurityContext;
import uk.org.hecaton.security.impl.PermissionSyncService;
import uk.org.hecaton.security.impl.SecurityContextImpl;
import uk.org.hecaton.security.impl.SecurityServiceImpl;
import uk.org.hecaton.spi.impl.NodeEngine;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.eventservice.impl.EventServiceImpl;
import uk.org.hecaton.spi.impl.servicemanager.ServiceManager;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.version.MemberVersion;
import uk.org.hecaton.version.Version;
import uk.org.hecaton.wan.impl.WanReplicationService;
import uk.org.hecaton.wan.impl.WanReplicationServiceImpl;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static uk.org.hecaton.config.ConfigAccessor.getActiveMemberNetworkConfig;
import static uk.org.hecaton.config.InstanceTrackingConfig.InstanceTrackingProperties.LICENSED;
import static uk.org.hecaton.config.InstanceTrackingConfig.InstanceTrackingProperties.MODE;
import static uk.org.hecaton.config.InstanceTrackingConfig.InstanceTrackingProperties.PID;
import static uk.org.hecaton.config.InstanceTrackingConfig.InstanceTrackingProperties.PRODUCT;
import static uk.org.hecaton.config.InstanceTrackingConfig.InstanceTrackingProperties.START_TIMESTAMP;
import static uk.org.hecaton.config.InstanceTrackingConfig.InstanceTrackingProperties.VERSION;
import static uk.org.hecaton.internal.util.CollectionUtil.setOf;
import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static uk.org.hecaton.internal.util.InstanceTrackingUtil.writeInstanceTrackingFile;
import static uk.org.hecaton.internal.util.StringUtil.isNullOrEmpty;
import static uk.org.hecaton.internal.util.StringUtil.isNullOrEmptyAfterTrim;
import static uk.org.hecaton.jet.impl.util.Util.JET_IS_DISABLED_MESSAGE;
import static uk.org.hecaton.jet.impl.util.Util.checkJetIsEnabled;
import static uk.org.hecaton.map.impl.MapServiceConstructor.getDefaultMapServiceConstructor;

@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity", "checkstyle:classdataabstractioncoupling"})
public class HecatonNodeExtension implements NodeExtension {
    private static final String PLATFORM_LOGO = """
      o    o     o     o---o   o--o o      o---o     o     o----o o--o--o
      |    |    / \\       /         |     /         / \\    |         |
      o----o       o     o   o----o |    o             o   o----o    |
      |    |  *     \\   /           |     \\       *     \\       |    |
      o    o *       o o---o   o--o o----o o---o *       o o----o    o
      """.indent(4);

    private static final String COPYRIGHT_LINE = "Copyright (c) 2026, The Hecaton Authors. "
            + "Based on Hazelcast, Copyright (c) 2008-2026, Hazelcast, Inc. Licensed under the Apache License 2.0.";
    private static final String VERSION_AUTO_UPGRADE_EXECUTOR = "hz:cluster:version:auto:upgrade";
    private static final int AUTO_UPGRADE_ATTEMPTS = 60;

    protected final Node node;
    protected final ILogger logger;
    protected final ILogger logoLogger;
    protected final ILogger systemLogger;
    protected final List<ClusterVersionListener> clusterVersionListeners = new CopyOnWriteArrayList<>();
    protected JetServiceBackend jetServiceBackend;
    protected IntegrityChecker integrityChecker;

    private final MemoryStats memoryStats = new DefaultMemoryStats();
    private final Set<Version> supportedVersions;
    /** Null unless persistence is enabled. */
    private final PersistenceCapability persistence;
    /** Null unless CP Subsystem persistence is enabled; created on first use. */
    private volatile CPPersistenceServiceImpl cpPersistence;
    private volatile AuditlogService auditlogService;
    private final Map<EndpointQualifier, Optional<MemberSocketInterceptor>> socketInterceptors = new ConcurrentHashMap<>();

    private final HotRestartService noOpHotRestartService = new NoOpHotRestartService();
    private final InternalHotRestartService noOpInternalHotRestartService = new NoopInternalHotRestartService();

    public HecatonNodeExtension(Node node) {
        this.node = node;
        this.logger = node.getLogger(NodeExtension.class);
        this.logoLogger = node.getLogger("uk.org.hecaton.system.logo");
        this.systemLogger = node.getLogger("uk.org.hecaton.system");

        checkCPSubsystemAllowed();
        checkSecurityAllowed();
        checkPersistenceAllowed();
        checkLosslessRestartAllowed();
        checkCPPersistenceAllowed();
        checkDynamicConfigurationPersistenceAllowed();
        checkSqlCatalogPersistenceAllowed();
        persistence = node.getConfig().getPersistenceConfig().isEnabled()
                ? node.getMemberModules().require(PersistenceCapability.class, "Persistence")
                : null;

        if (node.getConfig().getJetConfig().isEnabled()) {
            jetServiceBackend = createService(JetServiceBackend.class);
        }

        integrityChecker = new IntegrityChecker(node.getConfig().getIntegrityCheckerConfig(), this.systemLogger);
        supportedVersions = supportedVersions(BuildInfoProvider.getBuildInfo());
    }

    private void checkCPSubsystemAllowed() {
        CPSubsystemConfig cpSubsystemConfig = node.getConfig().getCPSubsystemConfig();
        if (cpSubsystemConfig != null && cpSubsystemConfig.getCPMemberCount() != 0
                && !HecatonFeature.CP_SUBSYSTEM.isAvailable()) {
            throw new IllegalStateException(HecatonFeature.CP_SUBSYSTEM.notAvailableMessage());
        }
    }

    private void checkPersistenceAllowed() {
        PersistenceConfig persistenceConfig = node.getConfig().getPersistenceConfig();
        if (persistenceConfig != null && persistenceConfig.isEnabled() && !HecatonFeature.PERSISTENCE.isAvailable()) {
            throw new IllegalStateException(HecatonFeature.PERSISTENCE.notAvailableMessage());
        }
    }

    private void checkSecurityAllowed() {
        SymmetricEncryptionConfig symmetricEncryptionConfig =
                getActiveMemberNetworkConfig(node.getConfig()).getSymmetricEncryptionConfig();
        if (symmetricEncryptionConfig != null && symmetricEncryptionConfig.isEnabled()
                && !HecatonFeature.SYMMETRIC_ENCRYPTION.isAvailable()) {
            throw new IllegalStateException(HecatonFeature.SYMMETRIC_ENCRYPTION.notAvailableMessage());
        }
    }

    private void checkLosslessRestartAllowed() {
        JetConfig jetConfig = node.getConfig().getJetConfig();
        if (jetConfig.isLosslessRestartEnabled() && !HecatonFeature.JET_LOSSLESS_RESTART.isAvailable()) {
            throw new IllegalStateException(HecatonFeature.JET_LOSSLESS_RESTART.notAvailableMessage());
        }
        requirePersistence(jetConfig.isLosslessRestartEnabled(), "Jet lossless restart (F18)");
    }

    /** Features that keep their data with persistence (F10) need it enabled. */
    private void requirePersistence(boolean featureEnabled, String feature) {
        if (featureEnabled && !node.getConfig().getPersistenceConfig().isEnabled()) {
            throw new InvalidConfigurationException(feature + " needs persistence: enable persistence (F10) too");
        }
    }

    private void checkCPPersistenceAllowed() {
        if (node.getConfig().getCPSubsystemConfig().isPersistenceEnabled() && !HecatonFeature.CP_PERSISTENCE.isAvailable()) {
            throw new IllegalStateException(HecatonFeature.CP_PERSISTENCE.notAvailableMessage());
        }
    }

    protected void checkDynamicConfigurationPersistenceAllowed() {
        Config config = node.getConfig();
        if (config.getDynamicConfigurationConfig().isPersistenceEnabled()) {
            if (!HecatonFeature.DYNAMIC_CONFIGURATION_PERSISTENCE.isAvailable()) {
                throw new IllegalStateException(HecatonFeature.DYNAMIC_CONFIGURATION_PERSISTENCE.notAvailableMessage());
            }

            if (config.getConfigurationFile() == null || !config.getConfigurationFile().exists()) {
                throw new InvalidConfigurationException(
                        "Dynamic Configuration Persistence is enabled but config file couldn't be found."
                                + " This is probably because declarative configuration isn't used."
                );
            }
        }
    }

    protected void checkSqlCatalogPersistenceAllowed() {
        Config config = node.getConfig();
        if (config.getSqlConfig().isCatalogPersistenceEnabled() && !HecatonFeature.SQL_CATALOG_PERSISTENCE.isAvailable()) {
            throw new IllegalStateException(HecatonFeature.SQL_CATALOG_PERSISTENCE.notAvailableMessage());
        }
        requirePersistence(config.getSqlConfig().isCatalogPersistenceEnabled(), "SQL catalog persistence (F19)");
    }

    @Override
    public void beforeStart() {
        integrityChecker.checkIntegrity();
        if (!isNullOrEmptyAfterTrim(node.getConfig().getLicenseKey())) {
            systemLogger.info(HecatonFeature.LICENSE.notAvailableMessage());
        }

        if (jetServiceBackend != null) {
            systemLogger.info("Jet is enabled");
            // Configure the internal distributed objects.
            jetServiceBackend.configureJetInternalObjects(node.config.getStaticConfig(), node.getProperties());
        } else {
            systemLogger.info(JET_IS_DISABLED_MESSAGE);
        }
    }

    @Override
    public void printNodeInfo() {
        BuildInfo buildInfo = node.getBuildInfo();
        printBannersBeforeNodeInfo();
        printNodeInfoInternal(buildInfo);
    }

    @Override
    public void logInstanceTrackingMetadata() {
        InstanceTrackingConfig trackingConfig = node.getConfig().getInstanceTrackingConfig();
        if (trackingConfig.isEnabled()) {
            writeInstanceTrackingFile(trackingConfig.getFileName(), trackingConfig.getFormatPattern(),
                    getTrackingFileProperties(node.getBuildInfo()), systemLogger);
        }
    }

    /**
     * Returns a map with supported instance tracking properties.
     *
     * @param buildInfo this node's build information
     */
    @SuppressWarnings("checkstyle:magicnumber")
    protected Map<String, Object> getTrackingFileProperties(BuildInfo buildInfo) {
        Map<String, Object> props = MapUtil.createHashMap(6);
        props.put(PRODUCT.getPropertyName(), InstanceProductName.HECATON.getProductName());
        props.put(VERSION.getPropertyName(), buildInfo.getVersion());
        props.put(MODE.getPropertyName(), Boolean.getBoolean("hecaton.tracking.server")
                ? InstanceMode.SERVER.getModeName()
                : InstanceMode.EMBEDDED.getModeName());
        props.put(START_TIMESTAMP.getPropertyName(), System.currentTimeMillis());
        props.put(LICENSED.getPropertyName(), 0);
        props.put(PID.getPropertyName(), JVMUtil.getPid());
        return props;
    }

    protected void printBannersBeforeNodeInfo() {
        logoLogger.info('\n' + PLATFORM_LOGO);
        systemLogger.info(COPYRIGHT_LINE);
    }

    private void printNodeInfoInternal(BuildInfo buildInfo) {
        systemLogger.info(getEditionString() + " " + buildInfo.getProductVersion()
                + " (" + buildInfo.toBuildString() + ", based on " + BuildInfo.BASED_ON + ", cluster version "
                + buildInfo.getCodebaseVersion().asVersion() + ") starting at " + node.getThisAddress());
        systemLogger.info("Cluster name: " + node.getConfig().getClusterName());
        systemLogger.fine("Configured Hecaton Serialization version: %s", buildInfo.getSerializationVersion());
    }

    protected String getEditionString() {
        return "Hecaton Platform";
    }

    @Override
    public UUID createMemberUuid() {
        UUID uuid = NodeExtension.super.createMemberUuid();
        return persistence == null ? uuid : persistence.start(node, uuid);
    }

    @Override
    public void beforeJoin() {
        if (persistence != null) {
            persistence.beforeJoin();
        }
        if (cpPersistence() != null) {
            cpPersistence().beforeJoin();
        }
    }

    @Override
    public void afterStart() {
        if (persistence != null) {
            persistence.afterStart();
        }
        if (cpPersistence() != null) {
            cpPersistence().afterJoin();
        }
        UserCodeNamespaceService namespaceService = node.getNamespaceService();
        if (namespaceService.isEnabled() && node.getProperties().getBoolean(ClusterProperty.METRICS_USER_CODE_NAMESPACES)) {
            node.getNodeEngine().getMetricsRegistry().registerDynamicMetricsProvider(namespaceService);
        }
        if (jetServiceBackend != null) {
            jetServiceBackend.startScanningForJobs();
        }
        if (node.getNodeEngine().getWanReplicationService() instanceof WanReplicationServiceImpl wanReplicationService) {
            wanReplicationService.afterStart();
        }
    }

    @Override
    public boolean isStartCompleted() {
        return node.getClusterService().isJoined()
                && (persistence == null || persistence.internalHotRestartService().isStartCompleted())
                && (cpPersistence() == null || cpPersistence().isRestoreCompleted());
    }

    @Override
    public boolean isReady() {
        return node.getClusterService().isJoined();
    }

    @Override
    public InternalSecurityContext getSecurityContext() {
        return new SecurityContextImpl(node);
    }

    @Nullable
    @Override
    public InternalSecurityContext getSecurityContextOrNull() {
        return node.securityContext instanceof InternalSecurityContext context ? context : null;
    }

    @Override
    public SecurityService getSecurityService() {
        return node.getConfig().getSecurityConfig().isEnabled() ? new SecurityServiceImpl(node) : null;
    }

    @Override
    public InternalSerializationService createSerializationService() {
        return createSerializationService(false);
    }

    @Override
    public InternalSerializationService createCompatibilitySerializationService() {
        return createSerializationService(true);
    }

    @Override
    public MemberSchemaService createSchemaService() {
        return new MemberSchemaService();
    }

    /**
     * Creates a serialization service. The {@code isCompatibility} parameter defines
     * whether the serialization format used by the service will conform to the
     * 3.x or the 4.x format.
     *
     * @param isCompatibility {@code true} if the serialized format should conform to the
     *                 3.x serialization format, {@code false} otherwise
     * @return the serialization service
     */
    private InternalSerializationService createSerializationService(boolean isCompatibility) {
        InternalSerializationService ss;
        try {
            Config config = node.getConfig();
            ClassLoader configClassLoader = node.getConfigClassLoader();

            HecatonInstanceImpl hecatonInstance = node.hecatonInstance;
            PartitioningStrategy<?> partitioningStrategy = getPartitioningStrategy(configClassLoader);

            SerializationServiceBuilder builder = new DefaultSerializationServiceBuilder();
            SerializationConfig serializationConfig = config.getSerializationConfig() != null
                    ? config.getSerializationConfig() : new SerializationConfig();

            byte version = (byte) node.getProperties().getInteger(ClusterProperty.SERIALIZATION_VERSION);

            ss = builder.setClassLoader(configClassLoader)
                    .setConfig(serializationConfig)
                    .setManagedContext(hecatonInstance.managedContext)
                    .setPartitioningStrategy(partitioningStrategy)
                    .setHecatonInstance(hecatonInstance)
                    .setVersion(version)
                    .setSchemaService(node.getSchemaService())
                    .setNotActiveExceptionSupplier(HecatonInstanceNotActiveException::new)
                    .setVersionedSerializationEnabled(true)
                    .setClusterVersionAware(new CodebaseClusterVersionAware())
                    .isCompatibility(isCompatibility)
                    .setProperties(node.getProperties())
                    .build();
        } catch (Exception e) {
            throw rethrow(e);
        }
        return ss;
    }

    protected PartitioningStrategy getPartitioningStrategy(ClassLoader configClassLoader) throws Exception {
        String partitioningStrategyClassName = node.getProperties().getString(ClusterProperty.PARTITIONING_STRATEGY_CLASS);
        if (!isNullOrEmpty(partitioningStrategyClassName)) {
            return ClassLoaderUtil.newInstance(configClassLoader, partitioningStrategyClassName);
        } else {
            return new DefaultPartitioningStrategy();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T createService(Class<T> clazz, Object... params) {
        if (WanReplicationService.class.isAssignableFrom(clazz)) {
            return (T) new WanReplicationServiceImpl(node);
        } else if (ICacheService.class.isAssignableFrom(clazz)) {
            return (T) new CacheService();
        } else if (MapService.class.isAssignableFrom(clazz)) {
            return createMapService();
        } else if (JetServiceBackend.class.isAssignableFrom(clazz)) {
            return (T) new JetServiceBackend(node);
        } else if (ClusterWideConfigurationService.class.isAssignableFrom(clazz)) {
            return createConfigurationService(params[0]);
        }

        throw new IllegalArgumentException("Unknown service class: " + clazz);
    }

    @SuppressWarnings("unchecked")
    private <T> T createMapService() {
        ConstructorFunction<NodeEngine, MapService> constructor = getDefaultMapServiceConstructor();
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        return (T) constructor.createNew(nodeEngine);
    }

    @SuppressWarnings("unchecked")
    private <T> T createConfigurationService(Object nodeEngine) {
        if (!(nodeEngine instanceof NodeEngine)) {
            throw new IllegalArgumentException(
                    "While creating ConfigurationService expected NodeEngine as a parameter, but found: "
                            + nodeEngine.getClass().getName()
            );
        }
        return (T) new ClusterWideConfigurationService((NodeEngine) nodeEngine, new EmptyDynamicConfigListener());
    }

    @Override
    public Map<String, Object> createExtensionServices() {
        Map<String, Object> services = new HashMap<>();
        if (jetServiceBackend != null) {
            services.put(JetServiceBackend.SERVICE_NAME, jetServiceBackend);
            services.put(JobEventService.SERVICE_NAME, new JobEventService(node.getNodeEngine()));
        }
        if (node.getConfig().getSecurityConfig().isEnabled()) {
            services.put(PermissionSyncService.SERVICE_NAME, new PermissionSyncService(node));
        }
        if (persistence != null) {
            services.put(InternalHotRestartService.SERVICE_NAME, persistence.internalHotRestartService());
        }
        return services;
    }

    @Override
    public MemberSocketInterceptor getSocketInterceptor(EndpointQualifier endpointQualifier) {
        // one interceptor per endpoint, created and initialized when the endpoint first needs it; without
        // the advanced network there is one endpoint, which has no qualifier
        EndpointQualifier key = endpointQualifier == null ? EndpointQualifier.MEMBER : endpointQualifier;
        return socketInterceptors.computeIfAbsent(key, this::createSocketInterceptor).orElse(null);
    }

    private Optional<MemberSocketInterceptor> createSocketInterceptor(EndpointQualifier endpointQualifier) {
        AdvancedNetworkConfig advancedNetwork = node.getConfig().getAdvancedNetworkConfig();
        EndpointConfig endpoint = advancedNetwork.isEnabled() ? advancedNetwork.getEndpointConfigs().get(endpointQualifier) : null;
        SocketInterceptorConfig config = advancedNetwork.isEnabled()
                ? (endpoint == null ? null : endpoint.getSocketInterceptorConfig())
                : node.getConfig().getNetworkConfig().getSocketInterceptorConfig();
        if (config == null || !config.isEnabled()) {
            return Optional.empty();
        }
        Object implementation = config.getImplementation();
        if (implementation == null) {
            try {
                implementation = ClassLoaderUtil.newInstance(node.getConfigClassLoader(), config.getClassName());
            } catch (Exception e) {
                throw new InvalidConfigurationException("Cannot create the socket interceptor " + config.getClassName(), e);
            }
        }
        if (!(implementation instanceof MemberSocketInterceptor interceptor)) {
            throw new InvalidConfigurationException("The socket interceptor of a member must implement "
                    + MemberSocketInterceptor.class.getName() + ": " + implementation.getClass().getName());
        }
        interceptor.init(config.getProperties());
        return Optional.of(interceptor);
    }

    @Override
    public InboundHandler[] createInboundHandlers(EndpointQualifier qualifier,
                                                  ServerConnection connection, ServerContext serverContext) {
        NodeEngineImpl nodeEngine = node.nodeEngine;
        PacketDecoder decoder = new PacketDecoder(connection, nodeEngine.getPacketDispatcher());
        return new InboundHandler[]{decoder};
    }

    @Override
    public OutboundHandler[] createOutboundHandlers(EndpointQualifier qualifier,
                                                    ServerConnection connection, ServerContext serverContext) {
        return new OutboundHandler[]{new PacketEncoder()};
    }

    @Override
    public Function<EndpointQualifier, ChannelInitializer> createChannelInitializerFn(ServerContext serverContext) {
        ChannelInitializerFunction provider = new ChannelInitializerFunction(serverContext, node.getConfig());
        provider.init();
        return provider;
    }

    @Override
    public MemoryStats getMemoryStats() {
        MemberModules modules = node.getMemberModules();
        if (modules != null) {
            Optional<NativeMemoryCapability> nativeMemory = modules.find(NativeMemoryCapability.class);
            if (nativeMemory.isPresent()) {
                return nativeMemory.get().memoryStats();
            }
        }
        return memoryStats;
    }

    @Override
    public void beforeShutdown(boolean terminate) {
        if (jetServiceBackend != null && !terminate) {
            // shutdown jobs on graceful shutdown
            jetServiceBackend.shutDownJobs();
        }
    }

    @Override
    public void afterShutdown() {
        logger.info("Destroying node NodeExtension.");
        if (cpPersistence != null) {
            cpPersistence.close();
        }
    }

    @Override
    public void validateJoinRequest(JoinMessage joinMessage) {
        // check joining member's version is in defined supported versions.
        MemberVersion memberVersion = joinMessage.getMemberVersion();
        Version clusterVersion = node.getClusterService().getClusterVersion();
        if (joinMessage instanceof JoinRequest joinRequest && joinRequest.supportsVersion(clusterVersion)) {
            return;
        }
        // for split brain healing
        if (joinMessage instanceof SplitBrainJoinMessage && memberVersion.asVersion().equals(clusterVersion)) {
            return;
        }
        String msg = constructVersionMismatchMessage(memberVersion, clusterVersion);
        throw new VersionMismatchException(msg);
    }

    @Override
    public Set<Version> getSupportedVersions() {
        return supportedVersions;
    }

    /**
     * Rolling upgrades (F14): a member runs in a cluster of its own minor version or of the previous
     * minor version of the same major, so members can be replaced one by one with the next minor
     * version before the cluster version is raised.
     */
    static Set<Version> supportedVersions(BuildInfo buildInfo) {
        Version current = buildInfo.getCodebaseVersion().asVersion();
        MemberVersion previous = buildInfo.getPreviousVersion();
        if (previous != null && previous.getMajor() == current.getMajor()
                && previous.getMinor() == current.getMinor() - 1) {
            return setOf(previous.asVersion(), current);
        }
        return setOf(current);
    }

    /** The lowest codebase version of the members, or null if there are too few members for an automatic upgrade. */
    private Version lowestMemberVersion(Collection<Member> members) {
        if (members.size() < node.getProperties().getInteger(ClusterProperty.CLUSTER_VERSION_AUTO_UPGRADE_MIN_CLUSTER_SIZE)) {
            return null;
        }
        Version lowest = null;
        for (Member member : members) {
            Version version = member.getVersion().asVersion();
            lowest = lowest == null || version.isLessThan(lowest) ? version : lowest;
        }
        return lowest;
    }

    @Override
    public void scheduleClusterVersionAutoUpgrade() {
        if (node.getProperties().getBoolean(ClusterProperty.CLUSTER_VERSION_AUTO_UPGRADE_ENABLED) && node.isMaster()) {
            node.getNodeEngine().getExecutionService().execute(VERSION_AUTO_UPGRADE_EXECUTOR,
                    () -> autoUpgradeClusterVersion(AUTO_UPGRADE_ATTEMPTS));
        }
    }

    /**
     * Raises the cluster version to the lowest codebase version of the members, if that is newer;
     * retried while the cluster state cannot change (migrations, for example).
     */
    private void autoUpgradeClusterVersion(int attempts) {
        ClusterServiceImpl clusterService = node.getClusterService();
        Version lowest = lowestMemberVersion(clusterService.getMembers());
        Version clusterVersion = clusterService.getClusterVersion();
        if (!clusterService.isMaster() || !node.isRunning() || lowest == null || !lowest.isGreaterThan(clusterVersion)
                || !supportedVersions.contains(lowest)) {
            return;
        }
        try {
            logger.info("All members run version " + lowest + " or newer: upgrading the cluster version from "
                    + clusterVersion + " to " + lowest);
            clusterService.changeClusterVersion(lowest);
        } catch (RuntimeException e) {
            if (attempts > 1) {
                logger.fine("Cannot upgrade the cluster version yet, retrying: " + e);
                node.getNodeEngine().getExecutionService().schedule(VERSION_AUTO_UPGRADE_EXECUTOR,
                        () -> autoUpgradeClusterVersion(attempts - 1), 1, TimeUnit.SECONDS);
            } else {
                logger.warning("Cannot upgrade the cluster version to " + lowest, e);
            }
        }
    }

    private static String constructVersionMismatchMessage(MemberVersion memberVersion, Version clusterVersion) {
        String msg = "Joining node's version " + memberVersion + " is not compatible with cluster version " + clusterVersion;
        msg += " (a member can join a cluster of its own minor version or of the previous minor version;"
                + " see the rolling upgrade procedure in docs/operations.md)";
        return msg;
    }

    @Override
    public void beforeClusterStateChange(ClusterState currState, ClusterState requestedState, boolean isTransient) {
        if (jetServiceBackend != null) {
            jetServiceBackend.beforeClusterStateChange(requestedState);
        }
    }

    @Override
    public void onClusterStateChange(ClusterState newState, boolean isTransient) {
        ServiceManager serviceManager = node.getNodeEngine().getServiceManager();
        serviceManager.getServices(ClusterStateListener.class).forEach(listener -> listener.onClusterStateChange(newState));
        if (persistence != null && !isTransient) {
            persistence.onClusterStateChange(newState);
        }
    }

    @Override
    public void onPartitionStateChange() {
        ClusterViewListenerService service = node.clientEngine.getClusterViewListenerService();
        if (service != null) {
            service.onPartitionStateChange();
        }
        if (persistence != null) {
            persistence.onPartitionStateChange();
        }
    }

    @Override
    public void onMemberListChange() {
        ClusterViewListenerService service = node.clientEngine.getClusterViewListenerService();
        if (service != null) {
            service.onMemberListChange();
        }
        node.clusterTopologyIntentTracker.onMembershipChange();
        if (persistence != null) {
            persistence.onMemberListChange();
        }
    }

    @Override
    public void onClusterVersionChange(Version newVersion) {
        if (!node.getVersion().asVersion().isEqualTo(newVersion)) {
            systemLogger.info("Cluster version set to " + newVersion);
        }
        ServiceManager serviceManager = node.getNodeEngine().getServiceManager();
        serviceManager.getServices(ClusterVersionListener.class)
                .forEach(listener -> listener.onClusterVersionChange(newVersion));
        // also trigger cluster version change on explicitly registered listeners
        clusterVersionListeners.forEach(listener -> listener.onClusterVersionChange(newVersion));
        ClusterViewListenerService service = node.clientEngine.getClusterViewListenerService();
        if (service != null) {
            service.onClusterVersionChange();
        }
        if (persistence != null) {
            persistence.onClusterVersionChange(newVersion);
        }
    }

    /**
     * Hecaton members accept failover clients (F15): a client configured with
     * {@code ClientFailoverConfig} may connect and later move to another cluster.
     */
    @Override
    public boolean isClientFailoverSupported() {
        return true;
    }

    @Override
    public boolean isNodeVersionCompatibleWith(Version clusterVersion) {
        Preconditions.checkNotNull(clusterVersion);
        return getSupportedVersions().contains(clusterVersion);
    }

    @Override
    public boolean registerListener(Object listener) {
        if (listener instanceof HecatonInstanceAware aware) {
            aware.setHecatonInstance(node.hecatonInstance);
        }
        if (listener instanceof ClusterVersionListener clusterVersionListener) {
            clusterVersionListeners.add(clusterVersionListener);
            // on registration, invoke once the listening method so version is properly initialized on the listener
            clusterVersionListener.onClusterVersionChange(getClusterOrNodeVersion());
            return true;
        }
        return false;
    }

    @Override
    public HotRestartService getHotRestartService() {
        return persistence == null ? noOpHotRestartService : persistence.hotRestartService();
    }

    @Override
    public InternalHotRestartService getInternalHotRestartService() {
        return persistence == null ? noOpInternalHotRestartService : persistence.internalHotRestartService();
    }

    // obtain cluster version, if already initialized (not null)
    // otherwise, if overridden with ClusterProperty#INIT_CLUSTER_VERSION, use this one
    // otherwise, if not overridden, use current node's codebase version
    private Version getClusterOrNodeVersion() {
        if (node.getClusterService() != null && !node.getClusterService().getClusterVersion().isUnknown()) {
            return node.getClusterService().getClusterVersion();
        } else {
            String overriddenClusterVersion = node.getProperties().getString(ClusterProperty.INIT_CLUSTER_VERSION);
            return (overriddenClusterVersion != null) ? MemberVersion.of(overriddenClusterVersion).asVersion()
                    : node.getVersion().asVersion();
        }
    }

    @Override
    public TimedMemberStateFactory createTimedMemberStateFactory(HecatonInstanceImpl instance) {
        return new TimedMemberStateFactory(instance);
    }

    @Override
    public void registerPlugins(Diagnostics diagnostics) {
        final NodeEngineImpl nodeEngine = node.nodeEngine;
        HecatonProperties properties = nodeEngine.getProperties();

        // static loggers at beginning of file
        diagnostics.register(new BuildInfoPlugin(nodeEngine.getLogger(BuildInfoPlugin.class)));
        diagnostics.register(new SystemPropertiesPlugin(nodeEngine.getLogger(SystemPropertiesPlugin.class)));
        diagnostics.register(new ConfigPropertiesPlugin(nodeEngine));

        // periodic loggers
        diagnostics.register(new OverloadedConnectionsPlugin(nodeEngine));
        diagnostics.register(new EventQueuePlugin(nodeEngine.getLogger(EventQueuePlugin.class),
                ((EventServiceImpl) nodeEngine.getEventService()).getEventExecutor(), properties));
        diagnostics.register(new PendingInvocationsPlugin(nodeEngine.getLogger(PendingInvocationsPlugin.class),
                nodeEngine.getOperationService().getInvocationRegistry(), properties));
        diagnostics.register(new MetricsPlugin(nodeEngine.getLogger(MetricsPlugin.class),
                nodeEngine.getMetricsRegistry(), properties));
        diagnostics.register(new SlowOperationPlugin(nodeEngine.getLogger(SlowOperationPlugin.class),
                nodeEngine.getOperationService(), properties));
        diagnostics.register(new InvocationSamplePlugin(nodeEngine.getLogger(InvocationSamplePlugin.class),
                nodeEngine.getOperationService().getInvocationRegistry(), properties));
        diagnostics.register(new InvocationProfilerPlugin(nodeEngine.getLogger(InvocationProfilerPlugin.class),
                nodeEngine.getOperationService().getInvocationRegistry(), properties));
        diagnostics.register(new OperationProfilerPlugin(nodeEngine.getLogger(OperationProfilerPlugin.class),
                nodeEngine.getOperationService().getOpLatencyDistributions(), properties));
        diagnostics.register(new MemberHecatonInstanceInfoPlugin(nodeEngine));
        diagnostics.register(new SystemLogPlugin(nodeEngine.getLogger(SystemLogPlugin.class), properties, node.getServer(),
                nodeEngine.getHecatonInstance(), node.getNodeExtension()));
        diagnostics.register(new StoreLatencyPlugin(nodeEngine.getLogger(StoreLatencyPlugin.class), properties));
        diagnostics.register(new MemberHeartbeatPlugin(nodeEngine.getLogger(MemberHeartbeatPlugin.class),
                nodeEngine.getClusterService(), properties));
        diagnostics.register(new NetworkingImbalancePlugin(nodeEngine.getLogger(NetworkingImbalancePlugin.class),
                properties, node.getServer()));
        diagnostics.register(new OperationHeartbeatPlugin(nodeEngine.getLogger(OperationHeartbeatPlugin.class),
                nodeEngine.getOperationService().getInvocationMonitor(), properties));
        diagnostics.register(new OperationThreadSamplerPlugin(nodeEngine));
    }

    @Override
    public ManagementService createJMXManagementService(HecatonInstanceImpl instance) {
        return new ManagementService(instance);
    }

    @Override
    public TextCommandService createTextCommandService() {
        return new TextCommandServiceImpl(node);
    }

    @Override
    public CPPersistenceService getCPPersistenceService() {
        CPPersistenceService service = cpPersistence();
        return service != null ? service : NopCPPersistenceService.INSTANCE;
    }

    /** The CP Subsystem persistence service (F9), or null if it is not enabled. */
    private CPPersistenceServiceImpl cpPersistence() {
        if (!node.getConfig().getCPSubsystemConfig().isPersistenceEnabled()) {
            return null;
        }
        CPPersistenceServiceImpl service = cpPersistence;
        if (service == null) {
            synchronized (this) {
                service = cpPersistence;
                if (service == null) {
                    service = new CPPersistenceServiceImpl(node);
                    cpPersistence = service;
                }
            }
        }
        return service;
    }

    @Override
    public CPSubsystem createCPSubsystem(NodeEngine nodeEngine) {
        return new CPSubsystemImpl(nodeEngine);
    }

    /** F21: Hecaton has no licenses; a license key set at runtime is ignored. */
    public void setLicenseKey(String licenseKey) {
        systemLogger.info(HecatonFeature.LICENSE.notAvailableMessage());
    }

    @Override
    public AuditlogService getAuditlogService() {
        AuditlogService service = auditlogService;
        if (service == null) {
            synchronized (this) {
                service = auditlogService;
                if (service == null) {
                    service = createAuditlogService(node.getConfig().getAuditlogConfig());
                    auditlogService = service;
                }
            }
        }
        return service;
    }

    private AuditlogService createAuditlogService(AuditlogConfig config) {
        if (!config.isEnabled()) {
            return NoOpAuditlogService.INSTANCE;
        }
        try {
            AuditlogServiceFactory factory = config.getFactoryClassName() == null
                    ? new ILoggerAuditlogServiceFactory()
                    : ClassLoaderUtil.newInstance(node.getConfigClassLoader(), config.getFactoryClassName());
            factory.init(null, config.getProperties());
            return factory.createAuditlog();
        } catch (Exception e) {
            throw new InvalidConfigurationException("Cannot create the audit log " + config.getFactoryClassName(), e);
        }
    }

    @Override
    public JetService getJet() {
        checkJetIsEnabled(node.nodeEngine);
        return jetServiceBackend.getJet();
    }

    @Override
    @Nullable
    public JetServiceBackend getJetServiceBackend() {
        return jetServiceBackend;
    }

    @Override
    public SSLEngineFactory createSslEngineFactory(SSLConfig sslConfig) {
        return SSLEngineFactories.create(sslConfig, false, node.getConfigClassLoader());
    }

    @Override
    public void onThreadStart(Thread thread) {
        // Setup NodeEngine context for User Code Deployment Namespacing in operations
        NodeEngineThreadLocalContext.declareNodeEngineReference(node.getNodeEngine());
    }

    @Override
    public void onThreadStop(Thread thread) {
        // Destroy NodeEngine context from User Code Deployment Namespacing
        NodeEngineThreadLocalContext.destroyNodeEngineReference();
    }

    @Override
    public UserCodeNamespaceService getNamespaceService() {
        if (node.getConfig().getNamespacesConfig().isEnabled()) {
            return new UserCodeNamespaceServiceImpl(node.getConfigClassLoader(), node.getConfig().getNamespacesConfig(),
                    node.getLogger(UserCodeNamespaceService.class));
        }
        return new NoOpUserCodeNamespaceService(node.getConfigClassLoader());
    }

    @Override
    public TpcServerBootstrap createTpcServerBootstrap() {
        return new TpcServerBootstrapImpl(node);
    }

    @Override
    public ClientEngine createClientEngine() {
        return new ClientEngineImpl(node);
    }

    @Override
    public HealthMonitor createHealthMonitor() {
        return new HealthMonitor(node);
    }
}
