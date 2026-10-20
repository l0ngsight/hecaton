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

package uk.org.hecaton.client.impl.clientside;

import uk.org.hecaton.client.HecatonClientNotActiveException;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientNetworkConfig;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.config.SocketOptions;
import uk.org.hecaton.client.impl.ClientExtension;
import uk.org.hecaton.client.impl.connection.tcp.ClientPlainChannelInitializer;
import uk.org.hecaton.client.impl.proxy.ClientMapProxy;
import uk.org.hecaton.client.impl.spi.ClientClusterService;
import uk.org.hecaton.client.impl.spi.ClientProxyFactory;
import uk.org.hecaton.client.impl.spi.impl.ClientClusterServiceImpl;
import uk.org.hecaton.client.impl.spi.impl.listener.ClientCPGroupViewService;
import uk.org.hecaton.client.impl.spi.impl.listener.NoOpClientCPGroupViewService;
import uk.org.hecaton.client.map.impl.nearcache.NearCachedClientMapProxy;
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.config.InstanceTrackingConfig;
import uk.org.hecaton.config.InstanceTrackingConfig.InstanceMode;
import uk.org.hecaton.config.InstanceTrackingConfig.InstanceProductName;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.NearCacheConfig;
import uk.org.hecaton.config.SSLConfig;
import uk.org.hecaton.config.SerializationConfig;
import uk.org.hecaton.config.SocketInterceptorConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.CPSubsystem;
import uk.org.hecaton.cp.CPSubsystemStubImpl;
import uk.org.hecaton.cp.internal.session.ProxySessionManager;
import uk.org.hecaton.cp.internal.session.StubProxySessionManager;
import uk.org.hecaton.instance.BuildInfo;
import uk.org.hecaton.instance.BuildInfoProvider;
import uk.org.hecaton.internal.memory.DefaultMemoryStats;
import uk.org.hecaton.internal.memory.MemoryStats;
import uk.org.hecaton.internal.nearcache.NearCacheManager;
import uk.org.hecaton.internal.nearcache.impl.DefaultNearCacheManager;
import uk.org.hecaton.internal.networking.ChannelInitializer;
import uk.org.hecaton.internal.nio.ClassLoaderUtil;
import uk.org.hecaton.internal.nio.ssl.SSLEngineFactories;
import uk.org.hecaton.internal.nio.ssl.TlsChannelInitializer;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.SerializationServiceBuilder;
import uk.org.hecaton.internal.serialization.impl.DefaultSerializationServiceBuilder;
import uk.org.hecaton.internal.util.HecatonFeature;
import uk.org.hecaton.internal.util.JVMUtil;
import uk.org.hecaton.internal.util.MapUtil;
import uk.org.hecaton.jet.JetService;
import uk.org.hecaton.jet.impl.JetClientInstanceImpl;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.nio.SocketInterceptor;
import uk.org.hecaton.nio.ssl.SSLEngineFactory;
import uk.org.hecaton.partition.PartitioningStrategy;
import uk.org.hecaton.partition.strategy.DefaultPartitioningStrategy;
import uk.org.hecaton.spi.impl.executionservice.TaskScheduler;
import uk.org.hecaton.spi.properties.HecatonProperties;

import java.util.Map;

import static uk.org.hecaton.config.InstanceTrackingConfig.InstanceTrackingProperties.LICENSED;
import static uk.org.hecaton.config.InstanceTrackingConfig.InstanceTrackingProperties.MODE;
import static uk.org.hecaton.config.InstanceTrackingConfig.InstanceTrackingProperties.PID;
import static uk.org.hecaton.config.InstanceTrackingConfig.InstanceTrackingProperties.PRODUCT;
import static uk.org.hecaton.config.InstanceTrackingConfig.InstanceTrackingProperties.START_TIMESTAMP;
import static uk.org.hecaton.config.InstanceTrackingConfig.InstanceTrackingProperties.VERSION;
import static uk.org.hecaton.internal.config.ConfigValidator.checkNearCacheConfig;
import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;
import static uk.org.hecaton.internal.util.InstanceTrackingUtil.writeInstanceTrackingFile;
import static uk.org.hecaton.spi.properties.ClusterProperty.SOCKET_CLIENT_BUFFER_DIRECT;

@SuppressWarnings({"WeakerAccess", "checkstyle:ClassFanOutComplexity", "ClassDataAbstractionCoupling"})
public class HecatonClientExtension implements ClientExtension {

    protected static final ILogger LOGGER = Logger.getLogger(HecatonClientExtension.class);

    protected volatile HecatonClientInstanceImpl client;
    protected JetClientInstanceImpl jetClient;

    private final MemoryStats memoryStats = new DefaultMemoryStats();

    @Override
    public void beforeStart(HecatonClientInstanceImpl client) {
        this.client = client;
    }

    @Override
    public void afterStart(HecatonClientInstanceImpl client) {
        this.jetClient = new JetClientInstanceImpl(client);
    }

    @Override
    public void logInstanceTrackingMetadata() {
        InstanceTrackingConfig trackingConfig = client.getClientConfig().getInstanceTrackingConfig();
        if (trackingConfig.isEnabled()) {
            writeInstanceTrackingFile(trackingConfig.getFileName(), trackingConfig.getFormatPattern(),
                    getTrackingFileProperties(BuildInfoProvider.getBuildInfo()), LOGGER);
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
        props.put(PRODUCT.getPropertyName(), InstanceProductName.HECATON_CLIENT);
        props.put(VERSION.getPropertyName(), buildInfo.getVersion());
        props.put(MODE.getPropertyName(), InstanceMode.CLIENT.getModeName());
        props.put(START_TIMESTAMP.getPropertyName(), System.currentTimeMillis());
        props.put(LICENSED.getPropertyName(), 0);
        props.put(PID.getPropertyName(), JVMUtil.getPid());
        return props;
    }

    @Override
    public InternalSerializationService createSerializationService(byte version) {
        try {
            ClientConfig config = client.getClientConfig();
            ClassLoader configClassLoader = config.getClassLoader();

            HecatonInstance hecatonInstance = client;
            PartitioningStrategy partitioningStrategy = getPartitioningStrategy(configClassLoader);
            SerializationConfig serializationConfig = config.getSerializationConfig() != null
                    ? config.getSerializationConfig()
                    : new SerializationConfig();

            SerializationServiceBuilder builder = new DefaultSerializationServiceBuilder();
            if (version > 0) {
                builder.setVersion(version);
            }
            return builder
                    .setClassLoader(configClassLoader)
                    .setConfig(serializationConfig)
                    .setManagedContext(new HecatonClientManagedContext(client, config.getManagedContext()))
                    .setPartitioningStrategy(partitioningStrategy)
                    .setHecatonInstance(hecatonInstance)
                    .setNotActiveExceptionSupplier(HecatonClientNotActiveException::new)
                    .setSchemaService(client.getSchemaService())
                    .setProperties(client.getProperties())
                    .build();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected PartitioningStrategy getPartitioningStrategy(ClassLoader configClassLoader) throws Exception {
        // This method historically only fetched our strategy from the client's local System Properties - this
        //  behaviour is not correct (it should read the ClientConfig properties). By using ClientConfig#getProperty
        //  we will be checking the ClientConfig first, and otherwise falling back to a System Property.
        String partitioningStrategyClassName = client.getClientConfig().getProperty(
                ClientProperty.PARTITIONING_STRATEGY_CLASS.getName());
        if (partitioningStrategyClassName != null && !partitioningStrategyClassName.isEmpty()) {
            return ClassLoaderUtil.newInstance(configClassLoader, partitioningStrategyClassName);
        } else {
            return new DefaultPartitioningStrategy();
        }
    }

    @Override
    public SocketInterceptor createSocketInterceptor() {
        LOGGER.warning(HecatonFeature.SOCKET_INTERCEPTOR.notAvailableMessage());
        return null;
    }

    @Override
    public SocketInterceptor createSocketInterceptor(SocketInterceptorConfig socketInterceptorConfig) {
        if (socketInterceptorConfig != null && socketInterceptorConfig.isEnabled()) {
            LOGGER.warning(HecatonFeature.SOCKET_INTERCEPTOR.notAvailableMessage());
        }
        return null;
    }

    @Override
    public ChannelInitializer createChannelInitializer() {
        ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();
        SSLConfig sslConfig = networkConfig.getSSLConfig();
        SocketOptions socketOptions = networkConfig.getSocketOptions();
        return createChannelInitializer(sslConfig, socketOptions);
    }

    @Override
    public ChannelInitializer createChannelInitializer(SSLConfig sslConfig, SocketOptions socketOptions) {
        HecatonProperties properties = client.getProperties();
        boolean directBuffer = properties.getBoolean(SOCKET_CLIENT_BUFFER_DIRECT);
        ChannelInitializer plain = new ClientPlainChannelInitializer(socketOptions, directBuffer);
        if (sslConfig == null || !sslConfig.isEnabled()) {
            return plain;
        }
        SSLEngineFactory engineFactory = SSLEngineFactories.create(sslConfig, true, client.getClientConfig().getClassLoader());
        return new TlsChannelInitializer(plain, engineFactory);
    }

    @Override
    public <T> ClientProxyFactory createServiceProxyFactory(Class<T> service) {
        if (MapService.class.isAssignableFrom(service)) {
            return createClientMapProxyFactory();
        }
        throw new IllegalArgumentException("Proxy factory cannot be created. Unknown service: " + service);
    }

    @Override
    public MemoryStats getMemoryStats() {
        return memoryStats;
    }

    private ClientProxyFactory createClientMapProxyFactory() {
        return (id, context) -> {
            ClientConfig clientConfig = client.getClientConfig();
            NearCacheConfig nearCacheConfig = clientConfig.getNearCacheConfig(id);
            if (nearCacheConfig != null) {
                checkNearCacheConfig(id, nearCacheConfig, clientConfig.getNativeMemoryConfig(), true);
                return new NearCachedClientMapProxy(MapService.SERVICE_NAME, id, context);
            } else {
                return new ClientMapProxy(MapService.SERVICE_NAME, id, context);
            }
        };
    }

    @Override
    public NearCacheManager createNearCacheManager() {
        SerializationService ss = client.getSerializationService();
        TaskScheduler taskScheduler = client.getTaskScheduler();
        ClassLoader classLoader = client.getClientConfig().getClassLoader();
        HecatonProperties properties = client.getProperties();

        return new DefaultNearCacheManager(ss, taskScheduler, classLoader, properties);
    }

    @Override
    public JetService getJet() {
        return jetClient;
    }

    @Override
    public CPSubsystem createCPSubsystem(HecatonClientInstanceImpl hecatonClientInstance) {
        return new CPSubsystemStubImpl(true);
    }

    @Override
    public ProxySessionManager createProxySessionManager(HecatonClientInstanceImpl hecatonClientInstance) {
        return new StubProxySessionManager();
    }

    @Override
    public ClientClusterService createClientClusterService(HecatonClientInstanceImpl clientInstance) {
        if (clientInstance.getClientConfig().getNetworkConfig()
                .getClusterRoutingConfig().getRoutingMode() == RoutingMode.MULTI_MEMBER) {
            throw new InvalidConfigurationException(HecatonFeature.MULTI_MEMBER_ROUTING.notAvailableMessage());
        }
        return new ClientClusterServiceImpl(clientInstance);
    }

    @Override
    public ClientCPGroupViewService createClientCPGroupViewService(HecatonClientInstanceImpl hecatonClientInstance,
                                                                   boolean directToLeaderRoutingEnabled) {
        return new NoOpClientCPGroupViewService();
    }
}
