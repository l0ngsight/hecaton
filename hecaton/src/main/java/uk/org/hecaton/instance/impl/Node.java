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

import uk.org.hecaton.client.ClientListener;
import uk.org.hecaton.client.impl.ClientEngine;
import uk.org.hecaton.client.impl.ClientEngineImpl;
import uk.org.hecaton.client.impl.NoOpClientEngine;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.cluster.MembershipListener;
import uk.org.hecaton.cluster.impl.MemberImpl;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.DiscoveryConfig;
import uk.org.hecaton.config.DiscoveryStrategyConfig;
import uk.org.hecaton.config.EndpointConfig;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.ListenerConfig;
import uk.org.hecaton.config.MemberAttributeConfig;
import uk.org.hecaton.config.UserCodeDeploymentConfig;
import uk.org.hecaton.core.DistributedObjectListener;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.HecatonInstanceAware;
import uk.org.hecaton.core.LifecycleEvent.LifecycleState;
import uk.org.hecaton.core.LifecycleListener;
import uk.org.hecaton.cp.CPMember;
import uk.org.hecaton.cp.event.CPGroupAvailabilityListener;
import uk.org.hecaton.cp.event.CPMembershipListener;
import uk.org.hecaton.instance.AddressPicker;
import uk.org.hecaton.instance.BuildInfo;
import uk.org.hecaton.instance.BuildInfoProvider;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.instance.ProtocolType;
import uk.org.hecaton.internal.ascii.TextCommandService;
import uk.org.hecaton.internal.cluster.Joiner;
import uk.org.hecaton.internal.cluster.impl.ClusterJoinManager;
import uk.org.hecaton.internal.cluster.impl.ClusterServiceImpl;
import uk.org.hecaton.internal.cluster.impl.ConfigCheck;
import uk.org.hecaton.internal.cluster.impl.DiscoveryJoiner;
import uk.org.hecaton.internal.cluster.impl.JoinRequest;
import uk.org.hecaton.internal.cluster.impl.MulticastJoiner;
import uk.org.hecaton.internal.cluster.impl.MulticastService;
import uk.org.hecaton.internal.cluster.impl.SplitBrainJoinMessage;
import uk.org.hecaton.internal.cluster.impl.TcpIpJoiner;
import uk.org.hecaton.internal.cluster.impl.operations.OnJoinOp;
import uk.org.hecaton.internal.config.AliasedDiscoveryConfigUtils;
import uk.org.hecaton.internal.config.DiscoveryConfigReadOnly;
import uk.org.hecaton.internal.config.MemberAttributeConfigReadOnly;
import uk.org.hecaton.internal.diagnostics.HealthMonitor;
import uk.org.hecaton.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import uk.org.hecaton.internal.management.ManagementCenterService;
import uk.org.hecaton.internal.metrics.MetricsRegistry;
import uk.org.hecaton.internal.metrics.impl.MetricsConfigHelper;
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.namespace.UserCodeNamespaceService;
import uk.org.hecaton.internal.namespace.impl.NamespaceAwareClassLoader;
import uk.org.hecaton.internal.nio.ClassLoaderUtil;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.internal.partition.impl.InternalPartitionServiceImpl;
import uk.org.hecaton.internal.partition.impl.MigrationInterceptor;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.compact.schema.MemberSchemaService;
import uk.org.hecaton.internal.server.Server;
import uk.org.hecaton.internal.server.tcp.LocalAddressRegistry;
import uk.org.hecaton.internal.server.tcp.ServerSocketRegistry;
import uk.org.hecaton.internal.services.GracefulShutdownAwareService;
import uk.org.hecaton.internal.usercodedeployment.UserCodeDeploymentClassLoader;
import uk.org.hecaton.internal.util.Clock;
import uk.org.hecaton.internal.util.FutureUtil;
import uk.org.hecaton.internal.util.HecatonFeature;
import uk.org.hecaton.jet.impl.util.ReflectionUtils;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.logging.LoggingService;
import uk.org.hecaton.logging.impl.LoggingServiceImpl;
import uk.org.hecaton.partition.MigrationListener;
import uk.org.hecaton.partition.PartitionLostListener;
import uk.org.hecaton.security.Credentials;
import uk.org.hecaton.security.SecurityContext;
import uk.org.hecaton.security.SecurityService;
import uk.org.hecaton.spi.discovery.DiscoveryNode;
import uk.org.hecaton.spi.discovery.SimpleDiscoveryNode;
import uk.org.hecaton.spi.discovery.impl.DefaultDiscoveryService;
import uk.org.hecaton.spi.discovery.impl.DefaultDiscoveryServiceProvider;
import uk.org.hecaton.spi.discovery.integration.DiscoveryMode;
import uk.org.hecaton.spi.discovery.integration.DiscoveryService;
import uk.org.hecaton.spi.discovery.integration.DiscoveryServiceProvider;
import uk.org.hecaton.spi.discovery.integration.DiscoveryServiceSettings;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.eventservice.impl.operations.OnJoinRegistrationOperation;
import uk.org.hecaton.spi.impl.proxyservice.impl.ProxyServiceImpl;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.version.MemberVersion;
import uk.org.hecaton.version.Version;

import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static uk.org.hecaton.config.ConfigAccessor.getActiveMemberNetworkConfig;
import static uk.org.hecaton.instance.EndpointQualifier.CLIENT;
import static uk.org.hecaton.instance.EndpointQualifier.MEMBER;
import static uk.org.hecaton.internal.cluster.impl.MulticastService.createMulticastService;
import static uk.org.hecaton.internal.config.AliasedDiscoveryConfigUtils.allUsePublicAddress;
import static uk.org.hecaton.internal.config.ConfigValidator.checkAdvancedNetworkConfig;
import static uk.org.hecaton.internal.config.ConfigValidator.warnForUsageOfDeprecatedSymmetricEncryption;
import static uk.org.hecaton.internal.util.EmptyStatement.ignore;
import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static uk.org.hecaton.internal.util.FutureUtil.waitWithDeadline;
import static uk.org.hecaton.internal.util.ThreadUtil.createThreadName;
import static uk.org.hecaton.spi.properties.ClusterProperty.DISCOVERY_SPI_ENABLED;
import static uk.org.hecaton.spi.properties.ClusterProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED;
import static uk.org.hecaton.spi.properties.ClusterProperty.GRACEFUL_SHUTDOWN_MAX_WAIT;
import static uk.org.hecaton.spi.properties.ClusterProperty.LOGGING_ENABLE_DETAILS;
import static uk.org.hecaton.spi.properties.ClusterProperty.LOGGING_SHUTDOWN;
import static uk.org.hecaton.spi.properties.ClusterProperty.LOGGING_TYPE;
import static uk.org.hecaton.spi.properties.ClusterProperty.SHUTDOWNHOOK_ENABLED;
import static uk.org.hecaton.spi.properties.ClusterProperty.SHUTDOWNHOOK_POLICY;
import static java.lang.Thread.currentThread;
import static java.security.AccessController.doPrivileged;

@SuppressWarnings({"checkstyle:methodcount", "checkstyle:visibilitymodifier", "checkstyle:classdataabstractioncoupling",
        "checkstyle:classfanoutcomplexity"})
public class Node {

    // name of property used to inject ClusterTopologyIntentTracker in Discovery Service
    public static final String DISCOVERY_PROPERTY_CLUSTER_TOPOLOGY_INTENT_TRACKER =
            "hecaton.internal.discovery.cluster.topology.intent.tracker";

    private static final int THREAD_SLEEP_DURATION_MS = 500;
    private static final String GRACEFUL_SHUTDOWN_EXECUTOR_NAME = "hz:graceful-shutdown";

    public final HecatonInstanceImpl hecatonInstance;
    public final DynamicConfigurationAwareConfig config;
    public final NodeEngineImpl nodeEngine;
    public final ClientEngine clientEngine;
    public final InternalPartitionServiceImpl partitionService;
    public final ClusterServiceImpl clusterService;
    public final MulticastService multicastService;
    public final DiscoveryService discoveryService;
    public final TextCommandService textCommandService;
    public final LoggingServiceImpl loggingService;
    public final Server server;

    /**
     * Member-to-member address only.
     * When the Node is configured with multiple endpoints, this address still represents {@link ProtocolType#MEMBER}
     * For accessing a full address-map, see {@link AddressPicker#getPublicAddressMap()}
     */
    public final Address address;
    public final SecurityContext securityContext;

    final ClusterTopologyIntentTracker clusterTopologyIntentTracker;

    private final ILogger logger;
    private final AtomicBoolean shuttingDown = new AtomicBoolean();
    private final NodeShutdownHookThread shutdownHookThread;
    private final MemberSchemaService schemaService;
    private final InternalSerializationService serializationService;
    private final InternalSerializationService compatibilitySerializationService;
    private final ClassLoader configClassLoader;
    private final UserCodeNamespaceService userCodeNamespaceService;
    private final NodeExtension nodeExtension;
    private final HecatonProperties properties;
    private final BuildInfo buildInfo;
    private final HealthMonitor healthMonitor;
    private final Joiner joiner;
    private final LocalAddressRegistry localAddressRegistry;
    private ManagementCenterService managementCenterService;

    // it can be changed on cluster service reset see: ClusterServiceImpl#resetLocalMemberUuid
    private volatile UUID thisUuid;
    private volatile NodeState state = NodeState.STARTING;

    /**
     * Codebase version of Hecaton being executed at this Node, as resolved by {@link BuildInfoProvider}.
     * For example, when running on hecaton-3.8.jar, this would resolve to {@code Version.of(3,8,0)}.
     * A node's codebase version may be different from cluster version.
     */
    private final MemberVersion version;

    @SuppressWarnings({"checkstyle:executablestatementcount", "checkstyle:methodlength"})
    public Node(HecatonInstanceImpl hecatonInstance, Config staticConfig, NodeContext nodeContext) {
        MemberJavaVersion.check();
        this.properties = new HecatonProperties(staticConfig);
        DynamicConfigurationAwareConfig config = new DynamicConfigurationAwareConfig(staticConfig, this.properties);
        this.hecatonInstance = hecatonInstance;
        this.config = config;
        this.configClassLoader = generateConfigClassloader(config);

        String policy = properties.getString(SHUTDOWNHOOK_POLICY);
        this.shutdownHookThread = new NodeShutdownHookThread("hc.ShutdownThread", policy);
        // Calling getBuildInfo() instead of directly using BuildInfoProvider.BUILD_INFO.
        // Version can be overridden via system property. That's why BuildInfo should be parsed for each Node.
        this.buildInfo = BuildInfoProvider.getBuildInfo();
        this.version = MemberVersion.of(buildInfo.getVersion());

        String loggingType = properties.getString(LOGGING_TYPE);
        boolean detailsEnabled = properties.getBoolean(LOGGING_ENABLE_DETAILS);
        boolean shutdownLoggingOnHecatonShutdown = properties.getBoolean(LOGGING_SHUTDOWN);
        loggingService = new LoggingServiceImpl(config.getClusterName(), loggingType, buildInfo, detailsEnabled,
                shutdownLoggingOnHecatonShutdown, this);
        MetricsConfigHelper.overrideMemberMetricsConfig(staticConfig, getLogger(MetricsConfigHelper.class));

        checkAdvancedNetworkConfig(config);
        final AddressPicker addressPicker = nodeContext.createAddressPicker(this);
        try {
            addressPicker.pickAddress();
        } catch (Throwable e) {
            throw rethrow(e);
        }

        ServerSocketRegistry serverSocketRegistry = new ServerSocketRegistry(addressPicker.getServerSocketChannels(),
                !config.getAdvancedNetworkConfig().isEnabled());
        ILogger tmpLogger = null;

        try {
            boolean liteMember = config.isLiteMember();
            nodeExtension = nodeContext.createNodeExtension(this);
            address = addressPicker.getPublicAddress(MEMBER);
            thisUuid = nodeExtension.createMemberUuid();
            final Map<String, String> memberAttributes = findMemberAttributes(
                    new MemberAttributeConfigReadOnly(config.getMemberAttributeConfig()));
            MemberImpl localMember = new MemberImpl.Builder(addressPicker.getPublicAddressMap())
                    .version(version)
                    .localMember(true)
                    .uuid(thisUuid)
                    .attributes(memberAttributes)
                    .liteMember(liteMember)
                    .instance(hecatonInstance)
                    .build();
            loggingService.setThisMember(localMember);
            tmpLogger = loggingService.getLogger(Node.class.getName());
            logger = tmpLogger;

            nodeExtension.printNodeInfo();
            nodeExtension.beforeStart();
            nodeExtension.logInstanceTrackingMetadata();

            // Initialise NamespaceService early on, so Namespaces can be used ASAP
            userCodeNamespaceService = nodeExtension.getNamespaceService();
            schemaService = nodeExtension.createSchemaService();
            serializationService = nodeExtension.createSerializationService();
            compatibilitySerializationService = nodeExtension.createCompatibilitySerializationService();
            securityContext = config.getSecurityConfig().isEnabled() ? nodeExtension.getSecurityContext() : null;
            warnForUsageOfDeprecatedSymmetricEncryption(config, logger);
            nodeEngine = new NodeEngineImpl(this);
            config.setServices(nodeEngine);
            config.onSecurityServiceUpdated(getSecurityService());
            MetricsRegistry metricsRegistry = nodeEngine.getMetricsRegistry();
            metricsRegistry.provideMetrics(nodeExtension);
            localAddressRegistry = new LocalAddressRegistry(this, addressPicker);
            server = nodeContext.createServer(this, serverSocketRegistry, localAddressRegistry);
            healthMonitor = nodeExtension.createHealthMonitor();
            clientEngine = hasClientServerSocket() ? nodeExtension.createClientEngine() : new NoOpClientEngine();
            JoinConfig joinConfig = getActiveMemberNetworkConfig(this.config).getJoin();
            if (properties.getBoolean(ClusterProperty.PERSISTENCE_AUTO_CLUSTER_STATE)
                    && config.getPersistenceConfig().isEnabled()) {
                clusterTopologyIntentTracker = new KubernetesTopologyIntentTracker(this);
            } else {
                clusterTopologyIntentTracker = new NoOpClusterTopologyIntentTracker();
            }
            DiscoveryConfig discoveryConfig = new DiscoveryConfigReadOnly(joinConfig.getDiscoveryConfig());
            List<DiscoveryStrategyConfig> aliasedDiscoveryConfigs =
                    AliasedDiscoveryConfigUtils.createDiscoveryStrategyConfigs(joinConfig);
            boolean isAutoDetectionEnabled = joinConfig.isAutoDetectionEnabled();
            discoveryService = createDiscoveryService(discoveryConfig, aliasedDiscoveryConfigs, isAutoDetectionEnabled,
                    localMember);
            new NodeSecurityBanner(config, properties, shouldUseMulticastJoiner(joinConfig), loggingService)
                    .printSecurityInfo();
            clusterService = new ClusterServiceImpl(this, localMember);
            partitionService = new InternalPartitionServiceImpl(this);
            textCommandService = nodeExtension.createTextCommandService();
            multicastService = createMulticastService(addressPicker.getBindAddress(MEMBER), this, config, logger);
            joiner = nodeContext.createJoiner(this);
        } catch (Throwable e) {
            try {
                if (tmpLogger == null) {
                    tmpLogger = Logger.getLogger(Node.class);
                }
                tmpLogger.severe("Node creation failed", e);
            } catch (Exception e1) {
                e.addSuppressed(e1);
            }
            serverSocketRegistry.destroy();
            try {
                shutdownServices(true);
            } catch (Throwable ignored) {
                ignore(ignored);
            }
            throw rethrow(e);
        }
    }

    private boolean hasClientServerSocket() {
        if (!config.getAdvancedNetworkConfig().isEnabled()) {
            return true;
        }

        Map<EndpointQualifier, EndpointConfig> endpointConfigs = config.getAdvancedNetworkConfig().getEndpointConfigs();
        EndpointConfig clientEndpointConfig = endpointConfigs.get(CLIENT);

        return clientEndpointConfig != null;
    }

    private ClassLoader generateConfigClassloader(Config config) {
        ClassLoader parent = getLegacyUCDClassLoader(config);
        if (config.getNamespacesConfig().isEnabled()) {
            if (!HecatonFeature.USER_CODE_NAMESPACES.isAvailable()) {
                throw new IllegalStateException(HecatonFeature.USER_CODE_NAMESPACES.notAvailableMessage());
            }
            return new NamespaceAwareClassLoader(parent);
        }
        return parent;
    }

    /**
     * If legacy user code deployment feature is enabled, then constructs a classloader to facilitate
     * legacy UCD, otherwise returns the config's classloader.
     */
    static ClassLoader getLegacyUCDClassLoader(Config config) {
        UserCodeDeploymentConfig userCodeDeploymentConfig = config.getUserCodeDeploymentConfig();
        ClassLoader classLoader;
        if (userCodeDeploymentConfig.isEnabled()) {
            ClassLoader parent = config.getClassLoader();
            final ClassLoader theParent = parent == null ? Node.class.getClassLoader() : parent;
            classLoader = doPrivileged(new PrivilegedAction<UserCodeDeploymentClassLoader>() {
                @Override
                public UserCodeDeploymentClassLoader run() {
                    return new UserCodeDeploymentClassLoader(theParent);
                }
            });
        } else {
            classLoader = config.getClassLoader();
            if (classLoader == null) {
                classLoader = Node.class.getClassLoader();
            }
        }
        return classLoader;
    }

    public DiscoveryService createDiscoveryService(DiscoveryConfig discoveryConfig,
                                                   List<DiscoveryStrategyConfig> aliasedDiscoveryConfigs,
                                                   boolean isAutoDetectionEnabled, Member localMember) {
        DiscoveryServiceProvider factory = discoveryConfig.getDiscoveryServiceProvider();
        if (factory == null) {
            factory = new DefaultDiscoveryServiceProvider();
        }
        ILogger logger = getLogger(DiscoveryService.class);

        final Map attributes = new HashMap<>(localMember.getAttributes());
        attributes.put(DISCOVERY_PROPERTY_CLUSTER_TOPOLOGY_INTENT_TRACKER, clusterTopologyIntentTracker);
        DiscoveryNode thisDiscoveryNode = new SimpleDiscoveryNode(localMember.getAddress(), attributes);

        DiscoveryServiceSettings settings = new DiscoveryServiceSettings()
                .setConfigClassLoader(configClassLoader)
                .setLogger(logger)
                .setDiscoveryMode(DiscoveryMode.Member)
                .setDiscoveryConfig(discoveryConfig)
                .setAliasedDiscoveryConfigs(aliasedDiscoveryConfigs)
                .setAutoDetectionEnabled(isAutoDetectionEnabled)
                .setDiscoveryNode(thisDiscoveryNode);

        return factory.newDiscoveryService(settings);
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:methodlength"})
    private void initializeListeners(Config config) {
        for (final ListenerConfig listenerCfg : config.getListenerConfigs()) {
            Object listener = listenerCfg.getImplementation();
            if (listener == null) {
                try {
                    listener = ClassLoaderUtil.newInstance(NamespaceUtil.getDefaultClassloader(getNodeEngine()),
                            listenerCfg.getClassName());
                } catch (Exception e) {
                    logger.severe(e);
                }
            }
            if (listener instanceof HecatonInstanceAware aware) {
                aware.setHecatonInstance(hecatonInstance);
            }
            boolean known = false;
            if (listener instanceof DistributedObjectListener objectListener) {
                final ProxyServiceImpl proxyService = (ProxyServiceImpl) nodeEngine.getProxyService();
                proxyService.addProxyListener(objectListener);
                known = true;
            }
            if (listener instanceof MembershipListener membershipListener) {
                clusterService.addMembershipListener(membershipListener);
                known = true;
            }
            if (listener instanceof MigrationListener migrationListener) {
                partitionService.addMigrationListener(migrationListener);
                known = true;
            }
            if (listener instanceof PartitionLostListener lostListener) {
                partitionService.addPartitionLostListener(lostListener);
                known = true;
            }
            if (listener instanceof LifecycleListener lifecycleListener) {
                hecatonInstance.lifecycleService.addLifecycleListener(lifecycleListener);
                known = true;
            }
            if (listener instanceof ClientListener) {
                String serviceName = ClientEngineImpl.SERVICE_NAME;
                nodeEngine.getEventService().registerLocalListener(serviceName, serviceName, listener);
                known = true;
            }
            if (listener instanceof MigrationInterceptor interceptor) {
                partitionService.setMigrationInterceptor(interceptor);
                known = true;
            }
            if (listener instanceof CPMembershipListener membershipListener) {
                hecatonInstance.getCPSubsystem().addMembershipListener(membershipListener);
                known = true;
            }
            if (listener instanceof CPGroupAvailabilityListener availabilityListener) {
                hecatonInstance.getCPSubsystem().addGroupAvailabilityListener(availabilityListener);
                known = true;
            }
            if (nodeExtension.registerListener(listener)) {
                known = true;
            }
            if (listener != null && !known) {
                final String error = "Unknown listener type: " + listener.getClass();
                Throwable t = new IllegalArgumentException(error);
                logger.warning(error, t);
            }
        }
    }

    public ManagementCenterService getManagementCenterService() {
        return managementCenterService;
    }

    public InternalSerializationService getSerializationService() {
        return serializationService;
    }

    public InternalSerializationService getCompatibilitySerializationService() {
        return compatibilitySerializationService;
    }

    public MemberSchemaService getSchemaService() {
        return schemaService;
    }

    public ClusterServiceImpl getClusterService() {
        return clusterService;
    }

    public InternalPartitionService getPartitionService() {
        return partitionService;
    }

    public UserCodeNamespaceService getNamespaceService() {
        return userCodeNamespaceService;
    }

    public Address getMasterAddress() {
        return clusterService.getMasterAddress();
    }

    public Address getThisAddress() {
        return address;
    }

    public UUID getThisUuid() {
        return thisUuid;
    }

    public void setThisUuid(UUID uuid) {
        thisUuid = uuid;
    }

    public MemberImpl getLocalMember() {
        return clusterService.getLocalMember();
    }

    public boolean isMaster() {
        return clusterService.isMaster();
    }

    public SecurityService getSecurityService() {
        return nodeExtension.getSecurityService();
    }

    void start() {
        nodeEngine.start();
        initializeListeners(config);
        hecatonInstance.lifecycleService.fireLifecycleEvent(LifecycleState.STARTING);
        clusterService.sendLocalMembershipEvent();
        server.start();
        JoinConfig join = getActiveMemberNetworkConfig(config).getJoin();
        if (shouldUseMulticastJoiner(join)) {
            final Thread multicastServiceThread = new Thread(multicastService,
                    createThreadName(hecatonInstance.getName(), "MulticastThread"));
            multicastServiceThread.start();
        }
        if (properties.getBoolean(DISCOVERY_SPI_ENABLED) || isAnyAliasedConfigEnabled(join)
                || (join.isAutoDetectionEnabled() && !isEmptyDiscoveryStrategies())) {
            discoveryService.start();

            // Discover local metadata from environment and merge into member attributes
            mergeEnvironmentProvidedMemberMetadata();
        }

        if (properties.getBoolean(SHUTDOWNHOOK_ENABLED)) {
            logger.finest("Adding ShutdownHook");
            Runtime.getRuntime().addShutdownHook(shutdownHookThread);
        }
        state = NodeState.ACTIVE;

        nodeExtension.beforeJoin();
        join();
        int clusterSize = clusterService.getSize();
        if (getActiveMemberNetworkConfig(config).isPortAutoIncrement()
                && address.getPort() >= getActiveMemberNetworkConfig(config).getPort() + clusterSize) {
            logger.warning("Config seed port is " + getActiveMemberNetworkConfig(config).getPort()
                    + " and cluster size is " + clusterSize + ". Some of the ports seem occupied!");
        }
        try {
            managementCenterService = new ManagementCenterService(hecatonInstance);
        } catch (Exception e) {
            logger.warning("ManagementCenterService could not be constructed!", e);
        }
        nodeExtension.afterStart();
        healthMonitor.start();
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    public void shutdown(final boolean terminate) {
        long start = Clock.currentTimeMillis();
        logger.finest("We are being asked to shutdown when state = %s", state);
        if (nodeExtension != null) {
            nodeExtension.beforeShutdown(terminate);
        }
        if (!setShuttingDown()) {
            waitIfAlreadyShuttingDown();
            return;
        }

        if (!terminate) {
            int maxWaitSeconds = properties.getSeconds(GRACEFUL_SHUTDOWN_MAX_WAIT);
            callGracefulShutdownAwareServices(maxWaitSeconds);
        } else {
            logger.warning("Terminating forcefully...");
        }

        // set the joined=false first so that
        // threads do not process unnecessary
        // events, such as remove address
        clusterService.resetJoinState();
        try {
            if (properties.getBoolean(SHUTDOWNHOOK_ENABLED)) {
                Runtime.getRuntime().removeShutdownHook(shutdownHookThread);
            }
        } catch (Throwable ignored) {
            ignore(ignored);
        }

        try {
            discoveryService.destroy();
        } catch (Throwable ignored) {
            ignore(ignored);
        }

        try {
            shutdownServices(terminate);
            state = NodeState.SHUT_DOWN;
            logger.info("Hecaton Shutdown is completed in " + (Clock.currentTimeMillis() - start) + " ms.");
        } finally {
            if (state != NodeState.SHUT_DOWN) {
                shuttingDown.compareAndSet(true, false);
            }
            loggingService.shutdown();
        }
    }

    private void callGracefulShutdownAwareServices(final int maxWaitSeconds) {
        ExecutorService executor = nodeEngine.getExecutionService().getExecutor(GRACEFUL_SHUTDOWN_EXECUTOR_NAME);
        Collection<GracefulShutdownAwareService> services = nodeEngine.getServices(GracefulShutdownAwareService.class);
        Collection<Future> futures = new ArrayList<>(services.size());

        for (final GracefulShutdownAwareService service : services) {
            Future<?> future = executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        boolean success = service.onShutdown(maxWaitSeconds, TimeUnit.SECONDS);
                        if (success) {
                            logger.fine("Graceful shutdown completed for %s", service);
                        } else {
                            logger.warning("Graceful shutdown failed for " + service);
                        }
                    } catch (Throwable e) {
                        logger.severe("Graceful shutdown failed for " + service, e);
                    }
                }

                @Override
                public String toString() {
                    return "Graceful shutdown task for service [" + service.toString() + "]";
                }
            });
            futures.add(future);
        }
        try {
            waitWithDeadline(futures, maxWaitSeconds, TimeUnit.SECONDS, FutureUtil.RETHROW_EVERYTHING);
        } catch (Exception e) {
            logger.warning(e);
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private void shutdownServices(boolean terminate) {
        if (nodeExtension != null) {
            nodeExtension.shutdown();
        }
        if (textCommandService != null) {
            textCommandService.stop();
        }
        if (multicastService != null) {
            logger.info("Shutting down multicast service...");
            multicastService.stop();
        }
        if (server != null) {
            logger.info("Shutting down connection manager...");
            server.shutdown();
        }

        if (nodeEngine != null) {
            logger.info("Shutting down node engine...");
            nodeEngine.shutdown(terminate);
        }

        if (securityContext != null) {
            securityContext.destroy();
        }
        if (serializationService != null) {
            logger.finest("Destroying serialization service...");
            serializationService.dispose();
        }

        if (nodeExtension != null) {
            nodeExtension.afterShutdown();
        }
        if (healthMonitor != null) {
            healthMonitor.stop();
        }
    }

    private void mergeEnvironmentProvidedMemberMetadata() {
        MemberImpl localMember = getLocalMember();
        Map<String, String> metadata = discoveryService.discoverLocalMetadata();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            Object value = entry.getValue();
            localMember.setAttribute(entry.getKey(), value.toString());
        }
    }

    public boolean setShuttingDown() {
        if (shuttingDown.compareAndSet(false, true)) {
            state = NodeState.PASSIVE;
            return true;
        }
        return false;
    }

    /**
     * Indicates that node is not shutting down, or it has not already shut down
     *
     * @return true if node is not shutting down, or it has not already shut down
     */
    public boolean isRunning() {
        return !shuttingDown.get();
    }

    private void waitIfAlreadyShuttingDown() {
        if (!shuttingDown.get()) {
            return;
        }
        logger.info("Node is already shutting down... Waiting for shutdown process to complete...");
        while (state != NodeState.SHUT_DOWN && shuttingDown.get()) {
            try {
                Thread.sleep(THREAD_SLEEP_DURATION_MS);
            } catch (InterruptedException e) {
                currentThread().interrupt();
                logger.warning("Interrupted while waiting for shutdown!");
                return;
            }
        }
        if (state != NodeState.SHUT_DOWN) {
            throw new IllegalStateException("Node failed to shutdown!");
        }
    }

    public void changeNodeStateToActive() {
        final ClusterState clusterState = clusterService.getClusterState();
        if (clusterState == ClusterState.PASSIVE) {
            throw new IllegalStateException("This method can be called only when cluster-state is not " + clusterState);
        }
        state = NodeState.ACTIVE;
    }

    public void changeNodeStateToPassive() {
        final ClusterState clusterState = clusterService.getClusterState();
        if (clusterState != ClusterState.PASSIVE) {
            throw new IllegalStateException("This method can be called only when cluster-state is " + clusterState);
        }
        state = NodeState.PASSIVE;
    }

    public void forceNodeStateToPassive() {
        state = NodeState.PASSIVE;
    }

    /**
     * Resets the internal cluster-state of the Node to be able to make it ready to join a new cluster.
     * After this method is called,
     * a new join process can be triggered by calling {@link #join()}.
     * <p>
     * This method is called during merge process after a split-brain is detected.
     */
    public void reset() {
        state = NodeState.ACTIVE;
        clusterService.resetJoinState();
        joiner.reset();
    }

    public LoggingService getLoggingService() {
        return loggingService;
    }

    public ILogger getLogger(String name) {
        return loggingService.getLogger(name);
    }

    public ILogger getLogger(Class<?> clazz) {
        return loggingService.getLogger(clazz);
    }

    public HecatonProperties getProperties() {
        return properties;
    }

    public TextCommandService getTextCommandService() {
        return textCommandService;
    }

    public Server getServer() {
        return server;
    }

    public ClassLoader getConfigClassLoader() {
        return configClassLoader;
    }

    public NodeEngineImpl getNodeEngine() {
        return nodeEngine;
    }

    public ClientEngine getClientEngine() {
        return clientEngine;
    }

    public NodeExtension getNodeExtension() {
        return nodeExtension;
    }

    public DiscoveryService getDiscoveryService() {
        return discoveryService;
    }

    public LocalAddressRegistry getLocalAddressRegistry() {
        return localAddressRegistry;
    }

    private enum ShutdownHookPolicy {
        TERMINATE,
        GRACEFUL
    }

    public class NodeShutdownHookThread extends Thread {
        private final ShutdownHookPolicy policy;

        NodeShutdownHookThread(String name, String policy) {
            super(name);
            this.policy = ShutdownHookPolicy.valueOf(policy);
        }

        @Override
        public void run() {
            try {
                if (!isRunning()) {
                    return;
                }
                final ClusterTopologyIntent shutdownIntent = clusterTopologyIntentTracker.getClusterTopologyIntent();
                if (clusterTopologyIntentTracker.isEnabled()
                        && getNodeExtension().getInternalHotRestartService().isEnabled()
                        && shutdownIntent != ClusterTopologyIntent.IN_MANAGED_CONTEXT_UNKNOWN
                        && shutdownIntent != ClusterTopologyIntent.NOT_IN_MANAGED_CONTEXT) {
                    final ClusterState clusterState = clusterService.getClusterState();
                    logger.info("Running shutdown hook... Current node state: " + state
                            + ", detected shutdown intent: " + shutdownIntent
                            + ", cluster state: " + clusterState);
                    clusterTopologyIntentTracker.shutdownWithIntent(shutdownIntent);
                } else {
                    logger.info("Running shutdown hook... Current node state: " + state);
                }
                switch (policy) {
                    case TERMINATE:
                        hecatonInstance.getLifecycleService().terminate();
                        break;
                    case GRACEFUL:
                        hecatonInstance.getLifecycleService().shutdown();
                        break;
                    default:
                        throw new IllegalArgumentException("Unimplemented shutdown hook policy: " + policy);
                }
            } catch (Exception e) {
                logger.warning(e);
            }
        }
    }

    public ClusterTopologyIntent getClusterTopologyIntent() {
        return clusterTopologyIntentTracker.getClusterTopologyIntent();
    }

    public void initializeClusterTopologyIntent(ClusterTopologyIntent clusterTopologyIntent) {
        clusterTopologyIntentTracker.initializeClusterTopologyIntent(clusterTopologyIntent);
    }

    // true when Hecaton is running in managed context with a tracker enabled
    public boolean isClusterStateManagementAutomatic() {
        return clusterTopologyIntentTracker.isEnabled();
    }

    /**
     * When running in managed context with automatic cluster state management,
     * return true if the current cluster size (as observed by Hecaton's cluster service)
     * is same as the cluster specification size (as provided by the runtime environment ie Kubernetes).
     * @return  {@code true} if running with auto cluster state management and current cluster size is same as
     *          specified one, otherwise {@code false}.
     */
    public boolean isClusterComplete() {
        return clusterTopologyIntentTracker.isEnabled()
                && clusterTopologyIntentTracker.getCurrentSpecifiedReplicaCount() == clusterService.getSize();
    }

    public boolean isManagedClusterStable() {
        return isClusterComplete()
                && clusterTopologyIntentTracker.getClusterTopologyIntent() == ClusterTopologyIntent.CLUSTER_STABLE;
    }

    public int currentSpecifiedReplicaCount() {
        return clusterTopologyIntentTracker.getCurrentSpecifiedReplicaCount();
    }

    public SplitBrainJoinMessage createSplitBrainJoinMessage() {
        MemberImpl localMember = getLocalMember();
        boolean liteMember = localMember.isLiteMember();
        Collection<Address> memberAddresses = clusterService.getMemberAddresses();
        int dataMemberCount = clusterService.getSize(DATA_MEMBER_SELECTOR);
        Version clusterVersion = clusterService.getClusterVersion();
        int memberListVersion = clusterService.getMembershipManager().getMemberListVersion();
        return  new SplitBrainJoinMessage(buildInfo.getBuildNumber(), version, address, localMember.getUuid(),
                liteMember, createConfigCheck(), memberAddresses, dataMemberCount, clusterVersion, memberListVersion);
    }

    public JoinRequest createJoinRequest(Address remoteAddress) {
        final Credentials credentials = (remoteAddress != null && securityContext != null)
                ? securityContext.getCredentialsFactory().newCredentials(remoteAddress) : null;
        final Set<UUID> excludedMemberUuids = nodeExtension.getInternalHotRestartService().getExcludedMemberUuids();

        MemberImpl localMember = getLocalMember();
        CPMember localCPMember = getLocalCPMember();
        UUID cpMemberUUID = localCPMember != null ? localCPMember.getUuid() : null;
        OnJoinRegistrationOperation preJoinOps = nodeEngine.getEventService().getPreJoinOperation();
        OnJoinOp onJoinOp = preJoinOps != null ? new OnJoinOp(Collections.singletonList(preJoinOps)) : null;
        return new JoinRequest(buildInfo.getBuildNumber(), version, address, localMember, createConfigCheck(),
                credentials, excludedMemberUuids, cpMemberUUID, onJoinOp, nodeExtension.getSupportedVersions());
    }

    private CPMember getLocalCPMember() {
        try {
            return hecatonInstance.getCPSubsystem().getLocalCPMember();
        } catch (HecatonException e) {
            return null;
        }
    }

    public ConfigCheck createConfigCheck() {
        String joinerType = joiner == null ? "" : joiner.getType();
        return new ConfigCheck(config, joinerType);
    }

    public void join() {
        if (clusterService.isJoined()) {
            if (logger.isFinestEnabled()) {
                logger.finest("Calling join on already joined node. ", ReflectionUtils.getStackTrace(Thread.currentThread()));
            } else {
                logger.warning("Calling join on already joined node. ");
            }
            return;
        }
        if (joiner == null) {
            logger.warning("No join method is enabled! Starting standalone.");
            ClusterJoinManager clusterJoinManager = clusterService.getClusterJoinManager();
            clusterJoinManager.setThisMemberAsMaster();
            return;
        }

        try {
            clusterService.resetJoinState();
            joiner.join();
        } catch (Throwable e) {
            logger.severe("Error while joining the cluster!", e);
        }

        if (!clusterService.isJoined()) {
            logger.severe("Could not join cluster. Shutting down now!");
            hecatonInstance.getLifecycleService().terminate();
        }
    }

    public Joiner getJoiner() {
        return joiner;
    }

    Joiner createJoiner() {
        JoinConfig join = getActiveMemberNetworkConfig(config).getJoin();
        join.verify();

        if (shouldUseMulticastJoiner(join) && multicastService != null) {
            logger.info("Using Multicast discovery");
            return new MulticastJoiner(this);
        } else if (join.getTcpIpConfig().isEnabled()) {
            logger.info("Using TCP/IP discovery");
            return new TcpIpJoiner(this);
        } else if (properties.getBoolean(DISCOVERY_SPI_ENABLED) || isAnyAliasedConfigEnabled(join)
                || join.isAutoDetectionEnabled()) {
            logger.info("Using Discovery SPI");
            return new DiscoveryJoiner(this, discoveryService, usePublicAddress(join));
        }
        return null;
    }

    public boolean shouldUseMulticastJoiner(JoinConfig join) {
        return join.getMulticastConfig().isEnabled()
                || (join.isAutoDetectionEnabled() && isEmptyDiscoveryStrategies());
    }

    private boolean isEmptyDiscoveryStrategies() {
        return discoveryService instanceof DefaultDiscoveryService dds
                && !dds.getDiscoveryStrategies().iterator().hasNext();
    }

    private static boolean isAnyAliasedConfigEnabled(JoinConfig join) {
        return !AliasedDiscoveryConfigUtils.createDiscoveryStrategyConfigs(join).isEmpty();
    }

    private boolean usePublicAddress(JoinConfig join) {
        return properties.getBoolean(DISCOVERY_SPI_PUBLIC_IP_ENABLED)
                || allUsePublicAddress(AliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom(join));
    }

    public Config getConfig() {
        return config;
    }

    /**
     * Returns the node state.
     *
     * @return current state of the node
     */
    public NodeState getState() {
        return state;
    }

    /**
     * Returns the codebase version of the node.
     *
     * @return codebase version of the node
     */
    public MemberVersion getVersion() {
        return version;
    }

    public boolean isLiteMember() {
        return getLocalMember().isLiteMember();
    }

    @Override
    public String toString() {
        return "Node[" + hecatonInstance.getName() + "]";
    }

    public BuildInfo getBuildInfo() {
        return buildInfo;
    }

    private Map<String, String> findMemberAttributes(MemberAttributeConfig attributeConfig) {
        Map<String, String> attributes = new HashMap<>(attributeConfig.getAttributes());
        Properties properties = System.getProperties();
        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith("hecaton.member.attribute.")) {
                String shortKey = key.substring("hecaton.member.attribute.".length());
                String value = properties.getProperty(key);
                attributes.put(shortKey, value);
            }
        }
        return attributes;
    }
}
