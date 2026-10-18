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

package uk.org.hecaton.spi.impl;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.impl.MemberImpl;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.WanReplicationRef;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.dataconnection.impl.DataConnectionServiceImpl;
import uk.org.hecaton.dataconnection.impl.InternalDataConnectionService;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.cluster.ClusterService;
import uk.org.hecaton.internal.diagnostics.Diagnostics;
import uk.org.hecaton.internal.dynamicconfig.ClusterWideConfigurationService;
import uk.org.hecaton.internal.dynamicconfig.ConfigurationService;
import uk.org.hecaton.internal.management.ManagementCenterService;
import uk.org.hecaton.internal.metrics.MetricsRegistry;
import uk.org.hecaton.internal.metrics.impl.MetricsConfigHelper;
import uk.org.hecaton.internal.metrics.impl.MetricsRegistryImpl;
import uk.org.hecaton.internal.metrics.metricsets.ClassLoadingMetricSet;
import uk.org.hecaton.internal.metrics.metricsets.FileMetricSet;
import uk.org.hecaton.internal.metrics.metricsets.GarbageCollectionMetricSet;
import uk.org.hecaton.internal.metrics.metricsets.OperatingSystemMetricSet;
import uk.org.hecaton.internal.metrics.metricsets.RuntimeMetricSet;
import uk.org.hecaton.internal.metrics.metricsets.ThreadMetricSet;
import uk.org.hecaton.internal.namespace.UserCodeNamespaceService;
import uk.org.hecaton.internal.nio.Packet;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.internal.partition.PartitionMigrationEvent;
import uk.org.hecaton.internal.partition.ReplicaSyncEvent;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.impl.compact.SchemaService;
import uk.org.hecaton.internal.serialization.impl.compact.schema.MemberSchemaService;
import uk.org.hecaton.internal.services.PostJoinAwareService;
import uk.org.hecaton.internal.services.PreJoinAwareService;
import uk.org.hecaton.internal.tpc.TpcServerBootstrap;
import uk.org.hecaton.internal.usercodedeployment.UserCodeDeploymentClassLoader;
import uk.org.hecaton.internal.usercodedeployment.UserCodeDeploymentService;
import uk.org.hecaton.internal.util.ConcurrencyDetection;
import uk.org.hecaton.internal.util.phonehome.PhoneHome;
import uk.org.hecaton.jet.impl.JetServiceBackend;
import uk.org.hecaton.jet.impl.util.Util;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.LoggingService;
import uk.org.hecaton.logging.impl.LoggingServiceImpl;
import uk.org.hecaton.spi.exception.RetryableHecatonException;
import uk.org.hecaton.spi.exception.ServiceNotFoundException;
import uk.org.hecaton.spi.impl.eventservice.EventService;
import uk.org.hecaton.spi.impl.eventservice.impl.EventServiceImpl;
import uk.org.hecaton.spi.impl.executionservice.ExecutionService;
import uk.org.hecaton.spi.impl.executionservice.impl.ExecutionServiceImpl;
import uk.org.hecaton.spi.impl.operationparker.OperationParker;
import uk.org.hecaton.spi.impl.operationparker.impl.OperationParkerImpl;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.spi.impl.operationservice.PartitionAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.impl.OperationServiceImpl;
import uk.org.hecaton.spi.impl.proxyservice.InternalProxyService;
import uk.org.hecaton.spi.impl.proxyservice.impl.ProxyServiceImpl;
import uk.org.hecaton.spi.impl.servicemanager.ServiceInfo;
import uk.org.hecaton.spi.impl.servicemanager.ServiceManager;
import uk.org.hecaton.spi.impl.servicemanager.impl.ServiceManagerImpl;
import uk.org.hecaton.spi.impl.tenantcontrol.impl.TenantControlServiceImpl;
import uk.org.hecaton.spi.merge.NamespaceAwareSplitBrainMergePolicyProvider;
import uk.org.hecaton.spi.merge.SplitBrainMergePolicyProvider;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.splitbrainprotection.impl.SplitBrainProtectionServiceImpl;
import uk.org.hecaton.sql.impl.InternalSqlService;
import uk.org.hecaton.sql.impl.MissingSqlService;
import uk.org.hecaton.transaction.TransactionManagerService;
import uk.org.hecaton.transaction.impl.TransactionManagerServiceImpl;
import uk.org.hecaton.version.MemberVersion;
import uk.org.hecaton.wan.impl.WanReplicationService;

import javax.annotation.Nonnull;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static uk.org.hecaton.internal.config.MergePolicyValidator.checkMapMergePolicy;
import static uk.org.hecaton.internal.metrics.MetricDescriptorConstants.MEMORY_PREFIX;
import static uk.org.hecaton.internal.metrics.impl.MetricsConfigHelper.memberMetricsLevel;
import static uk.org.hecaton.internal.util.EmptyStatement.ignore;
import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static uk.org.hecaton.spi.properties.ClusterProperty.BACKPRESSURE_ENABLED;
import static uk.org.hecaton.spi.properties.ClusterProperty.CONCURRENT_WINDOW_MS;

/**
 * The NodeEngineImpl is the where the construction of the Hecaton dependencies take place. It can be
 * compared to a Spring ApplicationContext. It is fine that we refer to concrete types, and it is fine
 * that we cast to a concrete type within this class (e.g. to call shutdown). In an application context
 * you get exactly the same behavior.
 * <p>
 * But the crucial thing is that we don't want to leak concrete dependencies to the outside. For example
 * we don't leak {@link uk.org.hecaton.spi.impl.operationservice.impl.OperationServiceImpl} to the outside.
 */
@SuppressWarnings({"checkstyle:classdataabstractioncoupling", "checkstyle:classfanoutcomplexity", "checkstyle:methodcount"})
public class NodeEngineImpl implements NodeEngine {

    private final Node node;
    private final SerializationService serializationService;
    private final SerializationService compatibilitySerializationService;
    private final LoggingServiceImpl loggingService;
    private final ILogger logger;
    private final MetricsRegistryImpl metricsRegistry;
    private final PhoneHome phoneHome;
    private final ProxyServiceImpl proxyService;
    private final ServiceManagerImpl serviceManager;
    private final ExecutionServiceImpl executionService;
    private final OperationServiceImpl operationService;
    private final EventServiceImpl eventService;
    private final OperationParkerImpl operationParker;
    private final ClusterWideConfigurationService configurationService;
    private final TransactionManagerServiceImpl transactionManagerService;
    private final WanReplicationService wanReplicationService;
    private final Consumer<Packet> packetDispatcher;
    private final SplitBrainProtectionServiceImpl splitBrainProtectionService;
    private final InternalSqlService sqlService;
    private final Diagnostics diagnostics;
    private final SplitBrainMergePolicyProvider splitBrainMergePolicyProvider;
    private final ConcurrencyDetection concurrencyDetection;
    private final TenantControlServiceImpl tenantControlService;
    private final InternalDataConnectionService dataConnectionService;
    private final TpcServerBootstrap tpcServerBootstrap;

    @SuppressWarnings("checkstyle:executablestatementcount")
    public NodeEngineImpl(Node node) {
        this.node = node;
        try {
            this.serializationService = node.getSerializationService();
            this.compatibilitySerializationService = node.getCompatibilitySerializationService();
            this.concurrencyDetection = newConcurrencyDetection();
            this.loggingService = node.loggingService;
            this.logger = node.getLogger(NodeEngine.class.getName());
            this.metricsRegistry = newMetricRegistry(node);
            this.phoneHome = new PhoneHome(node);
            this.proxyService = new ProxyServiceImpl(this);
            this.serviceManager = new ServiceManagerImpl(this);
            this.executionService = new ExecutionServiceImpl(this);
            this.tenantControlService = new TenantControlServiceImpl(this);
            this.tpcServerBootstrap = node.getNodeExtension().createTpcServerBootstrap();
            this.operationService = new OperationServiceImpl(this);
            this.eventService = new EventServiceImpl(this);
            this.operationParker = new OperationParkerImpl(this);
            UserCodeDeploymentService userCodeDeploymentService = new UserCodeDeploymentService();
            this.configurationService = node.getNodeExtension().createService(ClusterWideConfigurationService.class, this);
            ClassLoader configClassLoader = node.getConfigClassLoader();
            if (configClassLoader instanceof UserCodeDeploymentClassLoader loader) {
                loader.setUserCodeDeploymentService(userCodeDeploymentService);
            }
            this.transactionManagerService = new TransactionManagerServiceImpl(this);
            this.wanReplicationService = node.getNodeExtension().createService(WanReplicationService.class);
            this.sqlService = createSqlService();
            this.dataConnectionService = new DataConnectionServiceImpl(node, configClassLoader);
            this.packetDispatcher = new PacketDispatcher(
                    logger,
                    operationService.getOperationExecutor(),
                    operationService.getInboundResponseHandlerSupplier().get(),
                    operationService.getInvocationMonitor(),
                    eventService,
                    getJetPacketConsumer()
            );
            this.splitBrainProtectionService = new SplitBrainProtectionServiceImpl(this);
            this.diagnostics = newDiagnostics();
            this.splitBrainMergePolicyProvider = getConfig().getNamespacesConfig().isEnabled()
                    ? new NamespaceAwareSplitBrainMergePolicyProvider(this)
                    : new SplitBrainMergePolicyProvider(this.getConfigClassLoader());

            checkMapMergePolicies(node);


            serviceManager.registerService(OperationService.SERVICE_NAME, operationService);
            serviceManager.registerService(OperationParker.SERVICE_NAME, operationParker);
            serviceManager.registerService(UserCodeDeploymentService.SERVICE_NAME, userCodeDeploymentService);
            serviceManager.registerService(SchemaService.SERVICE_NAME, node.getSchemaService());
            serviceManager.registerService(ConfigurationService.SERVICE_NAME, configurationService);
            serviceManager.registerService(TenantControlServiceImpl.SERVICE_NAME, tenantControlService);
        } catch (Throwable e) {
            try {
                shutdown(true);
            } catch (Throwable ignored) {
                ignore(ignored);
            }
            throw rethrow(e);
        }
    }

    private InternalSqlService createSqlService() {
        if (!Util.isJetEnabled(this)) {
            return new MissingSqlService(node.getThisUuid(), false);
        }
        Class<?> clz;
        try {
            clz = Class.forName("uk.org.hecaton.sql.impl.SqlServiceImpl");
        } catch (ClassNotFoundException e) {
            // this is normal if the hecaton-sql module isn't present - return disabled service
            return new MissingSqlService(node.getThisUuid(), true);
        }

        try {
            Constructor<?> constructor = clz.getConstructor(getClass());
            return (InternalSqlService) constructor.newInstance(this);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException
                 | ClassCastException e) {
            // this isn't normal - we found the class, but there's something unexpected
            throw new RuntimeException(e);
        }
    }

    @Override
    public TpcServerBootstrap getTpcServerBootstrap() {
        return tpcServerBootstrap;
    }

    private void checkMapMergePolicies(Node node) {
        Map<String, MapConfig> mapConfigs = node.config.getMapConfigs();
        for (MapConfig mapConfig : mapConfigs.values()) {
            WanReplicationRef wanReplicationRef = mapConfig.getWanReplicationRef();
            if (wanReplicationRef != null) {
                String wanMergePolicyClassName = mapConfig.getWanReplicationRef().getMergePolicyClassName();
                checkMapMergePolicy(mapConfig,
                        wanMergePolicyClassName, splitBrainMergePolicyProvider);
            }

            String splitBrainMergePolicyClassName = mapConfig.getMergePolicyConfig().getPolicy();
            checkMapMergePolicy(mapConfig,
                    splitBrainMergePolicyClassName, splitBrainMergePolicyProvider);
        }
    }

    private ConcurrencyDetection newConcurrencyDetection() {
        HecatonProperties properties = node.getProperties();
        boolean writeThrough = properties.getBoolean(ClusterProperty.IO_WRITE_THROUGH_ENABLED);
        boolean backPressureEnabled = properties.getBoolean(BACKPRESSURE_ENABLED);

        if (writeThrough || backPressureEnabled) {
            return ConcurrencyDetection.createEnabled(properties.getInteger(CONCURRENT_WINDOW_MS));
        } else {
            return ConcurrencyDetection.createDisabled();
        }
    }

    private MetricsRegistryImpl newMetricRegistry(Node node) {
        return new MetricsRegistryImpl(getHecatonInstance().getName(), node.getLogger(MetricsRegistry.class),
                memberMetricsLevel(node.getProperties(), getLogger(MetricsConfigHelper.class)));
    }

    private Diagnostics newDiagnostics() {
        Address address = node.getThisAddress();
        String addressString = address.getHost().replace(":", "_") + "_" + address.getPort();
        String name = "diagnostics-" + addressString;

        return new Diagnostics(name, loggingService, getHecatonInstance().getName(),
                node.getProperties(), this);
    }

    @Override
    public LoggingService getLoggingService() {
        return loggingService;
    }

    @Override
    public MetricsRegistry getMetricsRegistry() {
        return metricsRegistry;
    }

    @Override
    public PhoneHome getPhoneHome() {
        return phoneHome;
    }

    public void start() {
        RuntimeMetricSet.register(metricsRegistry);
        GarbageCollectionMetricSet.register(metricsRegistry);
        OperatingSystemMetricSet.register(metricsRegistry);
        ThreadMetricSet.register(metricsRegistry);
        ClassLoadingMetricSet.register(metricsRegistry);
        FileMetricSet.register(metricsRegistry);

        metricsRegistry.registerStaticMetrics(node.getNodeExtension().getMemoryStats(), MEMORY_PREFIX);
        metricsRegistry.provideMetrics(operationService, proxyService, eventService, operationParker);

        serviceManager.start();
        proxyService.init();
        operationService.start();
        splitBrainProtectionService.start();
        sqlService.start();
        tpcServerBootstrap.start();
        diagnostics.start();
        node.getNodeExtension().registerPlugins(diagnostics);
    }

    public ConcurrencyDetection getConcurrencyDetection() {
        return concurrencyDetection;
    }

    public Consumer<Packet> getPacketDispatcher() {
        return packetDispatcher;
    }

    public Diagnostics getDiagnostics() {
        return diagnostics;
    }

    public ClusterWideConfigurationService getConfigurationService() {
        return configurationService;
    }

    public ServiceManager getServiceManager() {
        return serviceManager;
    }

    @Override
    public Address getThisAddress() {
        return node.getThisAddress();
    }

    @Override
    public Address getMasterAddress() {
        return node.getMasterAddress();
    }

    @Override
    public MemberImpl getLocalMember() {
        return node.getLocalMember();
    }

    @Override
    public Config getConfig() {
        return node.getConfig();
    }

    @Override
    public ClassLoader getConfigClassLoader() {
        return node.getConfigClassLoader();
    }

    @Override
    public EventService getEventService() {
        return eventService;
    }

    @Override
    public SerializationService getSerializationService() {
        return serializationService;
    }

    @Override
    public SerializationService getCompatibilitySerializationService() {
        return compatibilitySerializationService;
    }

    @Override
    public OperationServiceImpl getOperationService() {
        return operationService;
    }

    @Override
    public ExecutionService getExecutionService() {
        return executionService;
    }

    @Override
    public InternalPartitionService getPartitionService() {
        return node.getPartitionService();
    }

    @Override
    public ClusterService getClusterService() {
        return node.getClusterService();
    }

    @Override
    public ManagementCenterService getManagementCenterService() {
        return node.getManagementCenterService();
    }

    @Override
    public InternalProxyService getProxyService() {
        return proxyService;
    }

    @Override
    public TenantControlServiceImpl getTenantControlService() {
        return tenantControlService;
    }

    public OperationParker getOperationParker() {
        return operationParker;
    }

    @Override
    public WanReplicationService getWanReplicationService() {
        return wanReplicationService;
    }

    @Override
    public SplitBrainProtectionServiceImpl getSplitBrainProtectionService() {
        return splitBrainProtectionService;
    }

    @Override
    public InternalSqlService getSqlService() {
        return sqlService;
    }

    @Override
    public InternalDataConnectionService getDataConnectionService() {
        return dataConnectionService;
    }

    @Override
    public TransactionManagerService getTransactionManagerService() {
        return transactionManagerService;
    }

    @Override
    public Data toData(Object object) {
        return serializationService.toData(object);
    }

    @Override
    public <T> T toObject(Object object) {
        return serializationService.toObject(object);
    }

    @Override
    public <T> T toObject(Object object, Class klazz) {
        return serializationService.toObject(object, klazz);
    }

    @Override
    public boolean isRunning() {
        return node.isRunning();
    }

    @Override
    public boolean isStartCompleted() {
        return node.getNodeExtension().isStartCompleted();
    }

    @Override
    public HecatonInstance getHecatonInstance() {
        return node.hecatonInstance;
    }

    @Override
    public ILogger getLogger(String name) {
        return loggingService.getLogger(name);
    }

    @Override
    public ILogger getLogger(Class clazz) {
        return loggingService.getLogger(clazz);
    }

    @Override
    public HecatonProperties getProperties() {
        return node.getProperties();
    }

    @Override
    public <T> T getService(@Nonnull String serviceName) {
        T service = serviceManager.getService(serviceName);
        if (service == null) {
            if (isRunning()) {
                throw new HecatonException("Service with name '" + serviceName + "' not found!",
                        new ServiceNotFoundException("Service with name '" + serviceName + "' not found!"));
            } else {
                throw new RetryableHecatonException("HecatonInstance[" + getThisAddress() + "] is not active!");
            }
        }
        return service;
    }

    @Override
    public <T> T getServiceOrNull(@Nonnull String serviceName) {
        return serviceManager.getService(serviceName);
    }

    @Override
    public MemberVersion getVersion() {
        return node.getVersion();
    }

    @Override
    public SplitBrainMergePolicyProvider getSplitBrainMergePolicyProvider() {
        return splitBrainMergePolicyProvider;
    }

    @Override
    public <S> Collection<S> getServices(Class<S> serviceClass) {
        return serviceManager.getServices(serviceClass);
    }

    public Collection<ServiceInfo> getServiceInfos(Class serviceClass) {
        return serviceManager.getServiceInfos(serviceClass);
    }

    public void forEachMatchingService(Class serviceClass, Consumer<ServiceInfo> consumer) {
        serviceManager.forEachMatchingService(serviceClass, consumer);
    }

    @Override
    public Node getNode() {
        return node;
    }

    public void onMemberLeft(MemberImpl member) {
        operationParker.onMemberLeft(member);
        operationService.onMemberLeft(member);
        eventService.onMemberLeft(member);
    }

    @Override
    public void onClientDisconnected(UUID clientUuid) {
        operationParker.onClientDisconnected(clientUuid);
    }

    public void onPartitionMigrate(PartitionMigrationEvent migrationInfo) {
        operationParker.onPartitionMigrate(migrationInfo);
    }

    public void onReplicaSync(ReplicaSyncEvent syncEvent) {
        operationParker.onReplicaSync(syncEvent);
    }

    /**
     * Collects all post-join operations from {@link PostJoinAwareService}s.
     * <p>
     * Post join operations should return response, at least a {@code null} response.
     * <p>
     * <b>Note</b>: Post join operations must be lock free, meaning no locks at all:
     * no partition locks, no key-based locks, no service level locks, no database interaction!
     * The {@link Operation#getPartitionId()} method should return a negative value.
     * This means that the operations should not implement {@link PartitionAwareOperation}.
     *
     * @return the operations to be executed at the end of a finalized join
     */
    public Collection<Operation> getPostJoinOperations() {
        Collection<Operation> postJoinOps = new LinkedList<>();
        Collection<PostJoinAwareService> services = getServices(PostJoinAwareService.class);
        for (PostJoinAwareService service : services) {
            Operation postJoinOperation = service.getPostJoinOperation();
            if (postJoinOperation != null) {
                if (postJoinOperation.getPartitionId() >= 0) {
                    logger.severe("Post-join operations should not have partition ID set! Service: "
                            + service + ", Operation: "
                            + postJoinOperation);
                    continue;
                }
                postJoinOps.add(postJoinOperation);
            }
        }
        return postJoinOps;
    }

    public Collection<Operation> getPreJoinOperations() {
        Collection<Operation> preJoinOps = new LinkedList<>();
        Collection<PreJoinAwareService> services = getServices(PreJoinAwareService.class);
        for (PreJoinAwareService service : services) {
            Operation preJoinOperation = service.getPreJoinOperation();
            if (preJoinOperation != null) {
                if (preJoinOperation.getPartitionId() >= 0) {
                    logger.severe("Pre-join operations operations should not have partition ID set! Service: "
                            + service + ", Operation: "
                            + preJoinOperation);
                    continue;
                }
                preJoinOps.add(preJoinOperation);
            }
        }
        return preJoinOps;
    }

    public void reset() {
        sqlService.reset();
        operationParker.reset();
        operationService.reset();
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "cyclomaticcomplexity"})
    public void shutdown(boolean terminate) {
        logger.finest("Shutting down services...");
        if (sqlService != null) {
            sqlService.shutdown();
        }

        if (operationParker != null) {
            operationParker.shutdown();
        }
        if (operationService != null) {
            operationService.shutdownInvocations();
        }
        if (proxyService != null) {
            proxyService.shutdown();
        }
        if (serviceManager != null) {
            serviceManager.shutdown(terminate);
        }
        if (eventService != null) {
            eventService.shutdown();
        }
        if (operationService != null) {
            operationService.shutdownOperationExecutor();
        }
        if (wanReplicationService != null) {
            wanReplicationService.shutdown();
        }
        if (executionService != null) {
            executionService.shutdown();
        }
        if (tpcServerBootstrap != null) {
            tpcServerBootstrap.shutdown();
        }
        if (metricsRegistry != null) {
            metricsRegistry.shutdown();
        }
        if (phoneHome != null) {
            phoneHome.shutdown();
        }
        if (diagnostics != null) {
            diagnostics.shutdown();
        }
        if (dataConnectionService != null) {
            dataConnectionService.shutdown();
        }

    }

    @Override
    public MemberSchemaService getSchemaService() {
        return node.getSchemaService();
    }

    @Nonnull
    private Consumer<Packet> getJetPacketConsumer() {
        // Here, JetServiceBackend is not registered to service manager yet
        JetServiceBackend jetServiceBackend = node.getNodeExtension().getJetServiceBackend();
        if (jetServiceBackend != null) {
            return jetServiceBackend;
        } else {
            return packet -> {
                throw new UnsupportedOperationException("Jet is not enabled on this node");
            };
        }
    }

    @Override
    public UserCodeNamespaceService getNamespaceService() {
        return node.getNamespaceService();
    }
}
