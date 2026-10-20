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

package uk.org.hecaton.client.impl;

import uk.org.hecaton.client.config.SocketOptions;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.spi.ClientClusterService;
import uk.org.hecaton.client.impl.spi.ClientProxyFactory;
import uk.org.hecaton.client.impl.spi.impl.listener.ClientCPGroupViewService;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.config.SSLConfig;
import uk.org.hecaton.config.SocketInterceptorConfig;
import uk.org.hecaton.cp.CPSubsystem;
import uk.org.hecaton.cp.internal.session.ProxySessionManager;
import uk.org.hecaton.internal.nearcache.NearCacheManager;
import uk.org.hecaton.internal.networking.ChannelInitializer;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.jet.JetService;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.internal.memory.MemoryStats;
import uk.org.hecaton.nio.SocketInterceptor;

/**
 * ClientExtension is a client extension mechanism to be able to plug different implementations of
 * some modules, like; {@link InternalSerializationService} etc.
 */
public interface ClientExtension {

    /**
     * Called before client is started
     */
    void beforeStart(HecatonClientInstanceImpl client);

    /**
     * Called after node is started
     */
    void afterStart(HecatonClientInstanceImpl client);

    /**
     * Logs metadata about the instance to the configured instance tracking output.
     */
    void logInstanceTrackingMetadata();

    /**
     * Creates a {@link InternalSerializationService} instance to be used by this client.
     *
     * @param version serialization version to be created. Values less than 1 will be ignored and max supported version
     *                will be used
     * @return the created {@link InternalSerializationService} instance
     */
    InternalSerializationService createSerializationService(byte version);

    /**
     * Creates a {@link SocketInterceptor} to be used by this client if available,
     * otherwise returns <code>null</code>
     *
     * @return the created {@link SocketInterceptor} instance if available,
     * +          otherwise <code>null</code>
     */
    SocketInterceptor createSocketInterceptor();

    /**
     * Create socket interceptor according to given config
     *
     * @param socketInterceptorConfig config for socket interceptor
     * @return socket interceptor if it is able to created, null otherwise
     */
    SocketInterceptor createSocketInterceptor(SocketInterceptorConfig socketInterceptorConfig);

    ChannelInitializer createChannelInitializer();

    /**
     * @param sslConfig     ssl config for channel initializer
     * @param socketOptions socket options for channel initializer
     * @return @return ChannelInitializer created from given configs
     */
    ChannelInitializer createChannelInitializer(SSLConfig sslConfig, SocketOptions socketOptions);

    /**
     * Creates a {@link NearCacheManager} instance to be used by this client.
     *
     * @return the created {@link NearCacheManager} instance
     */
    NearCacheManager createNearCacheManager();

    /**
     * Creates a {@code ClientProxyFactory} for the supplied service class. Currently only the {@link MapService} is supported.
     *
     * @param service service for the proxy to create.
     * @return {@code ClientProxyFactory} for the service.
     * @throws java.lang.IllegalArgumentException if service is not known
     */
    <T> ClientProxyFactory createServiceProxyFactory(Class<T> service);

    /**
     * Returns MemoryStats of for the JVM and current HecatonInstance.
     *
     * @return memory statistics
     */
    MemoryStats getMemoryStats();

    /**
     * Returns a JetService.
     */
    JetService getJet();

    /**
     * Creates the relevant CP subsystem implementation.
     */
    CPSubsystem createCPSubsystem(HecatonClientInstanceImpl hecatonClientInstance);

    ProxySessionManager createProxySessionManager(HecatonClientInstanceImpl hecatonClientInstance);

    /**
     * Creates the relevant ClientClusterService implementation.
     *
     * @param clientInstance the client instance.
     * @throws InvalidConfigurationException if the service cannot be created due to an invalid configuration.
     * @return the ClientClusterService implementation.
     */
    ClientClusterService createClientClusterService(HecatonClientInstanceImpl clientInstance);

    /**
     * Creates the correct implementation of {@link ClientCPGroupViewService}.
     *
     * @param hecatonClientInstance the client instance
     * @return the concrete {@link ClientCPGroupViewService} implementation.
     */
    ClientCPGroupViewService createClientCPGroupViewService(HecatonClientInstanceImpl hecatonClientInstance,
                                                            boolean directToLeaderRoutingEnabled);
}
