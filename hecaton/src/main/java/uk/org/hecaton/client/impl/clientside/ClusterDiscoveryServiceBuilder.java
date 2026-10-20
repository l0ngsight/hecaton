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

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientNetworkConfig;
import uk.org.hecaton.client.config.ClientSecurityConfig;
import uk.org.hecaton.client.config.SocketOptions;
import uk.org.hecaton.client.config.impl.ClientAliasedDiscoveryConfigUtils;
import uk.org.hecaton.client.impl.ClientExtension;
import uk.org.hecaton.client.impl.connection.AddressProvider;
import uk.org.hecaton.client.impl.spi.ClientClusterService;
import uk.org.hecaton.client.impl.spi.impl.DefaultAddressProvider;
import uk.org.hecaton.client.impl.spi.impl.TranslateToPublicAddressProvider;
import uk.org.hecaton.client.impl.spi.impl.discovery.RemoteAddressProvider;
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.DiscoveryConfig;
import uk.org.hecaton.config.DiscoveryStrategyConfig;
import uk.org.hecaton.config.SSLConfig;
import uk.org.hecaton.config.SocketInterceptorConfig;
import uk.org.hecaton.config.security.StaticCredentialsFactory;
import uk.org.hecaton.core.LifecycleService;
import uk.org.hecaton.internal.config.DiscoveryConfigReadOnly;
import uk.org.hecaton.internal.config.RemovedCloudDiscovery;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.LoggingService;
import uk.org.hecaton.nio.SocketInterceptor;
import uk.org.hecaton.security.ICredentialsFactory;
import uk.org.hecaton.security.UsernamePasswordCredentials;
import uk.org.hecaton.spi.discovery.DiscoveryNode;
import uk.org.hecaton.spi.discovery.impl.DefaultDiscoveryService;
import uk.org.hecaton.spi.discovery.impl.DefaultDiscoveryServiceProvider;
import uk.org.hecaton.spi.discovery.integration.DiscoveryMode;
import uk.org.hecaton.spi.discovery.integration.DiscoveryService;
import uk.org.hecaton.spi.discovery.integration.DiscoveryServiceProvider;
import uk.org.hecaton.spi.discovery.integration.DiscoveryServiceSettings;
import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.spi.properties.HecatonProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static uk.org.hecaton.client.properties.ClientProperty.DISCOVERY_SPI_ENABLED;
import static uk.org.hecaton.internal.config.AliasedDiscoveryConfigUtils.allUsePublicAddress;
import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;
import static java.util.Collections.unmodifiableList;

class ClusterDiscoveryServiceBuilder {

    /**
     * Hazelcast Cloud (Viridian) discovery token property. Hecaton removed Hazelcast Cloud
     * discovery (D9); a client that still sets it fails with an explanation.
     */
    private static final HecatonProperty REMOVED_CLOUD_DISCOVERY_TOKEN =
            new HecatonProperty("hecaton.client.cloud.discovery.token");

    private final int configsTryCount;
    private final LoggingService loggingService;
    private final HecatonProperties properties;
    private final ClientExtension clientExtension;
    private final Collection<ClientConfig> configs;
    private final LifecycleService lifecycleService;
    private final AddressProvider externalAddressProvider;
    private final ClientClusterService clusterService;

    ClusterDiscoveryServiceBuilder(int configsTryCount, List<ClientConfig> configs, LoggingService loggingService,
                                   AddressProvider externalAddressProvider, HecatonProperties properties,
                                   ClientExtension clientExtension, LifecycleService lifecycleService,
                                   ClientClusterService clusterService) {
        this.configsTryCount = configsTryCount;
        this.configs = configs;
        this.loggingService = loggingService;
        this.externalAddressProvider = externalAddressProvider;
        this.properties = properties;
        this.clientExtension = clientExtension;
        this.lifecycleService = lifecycleService;
        this.clusterService = clusterService;
    }

    public ClusterDiscoveryService build() {
        ArrayList<CandidateClusterContext> contexts = new ArrayList<>();
        for (ClientConfig config : configs) {
            ClientNetworkConfig networkConfig = config.getNetworkConfig();
            SocketInterceptor interceptor = initSocketInterceptor(networkConfig.getSocketInterceptorConfig());
            ICredentialsFactory credentialsFactory = initCredentialsFactory(config);
            if (credentialsFactory == null) {
                credentialsFactory = new StaticCredentialsFactory(new UsernamePasswordCredentials(null, null));
            }
            credentialsFactory.configure(new ClientCallbackHandler(config));
            DiscoveryService discoveryService = initDiscoveryService(config);
            AddressProvider provider;
            if (externalAddressProvider != null) {
                provider = externalAddressProvider;
            } else {
                provider = createAddressProvider(config, discoveryService);
            }

            final SSLConfig sslConfig = networkConfig.getSSLConfig();
            final SocketOptions socketOptions = networkConfig.getSocketOptions();
            contexts.add(new CandidateClusterContext(config.getClusterName(), provider,
                    discoveryService, credentialsFactory,
                    interceptor, clientExtension.createChannelInitializer(sslConfig, socketOptions)));
        }
        return new ClusterDiscoveryService(unmodifiableList(contexts), configsTryCount, lifecycleService);
    }

    private AddressProvider createAddressProvider(ClientConfig clientConfig, DiscoveryService discoveryService) {
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();

        List<String> addresses = networkConfig.getAddresses();
        boolean addressListProvided = !addresses.isEmpty();
        boolean kubernetesDiscoveryEnabled = networkConfig.getKubernetesConfig() != null
                && networkConfig.getKubernetesConfig().isEnabled();
        boolean discoverySpiEnabled = discoverySpiEnabled(networkConfig);
        if (properties.getString(REMOVED_CLOUD_DISCOVERY_TOKEN) != null) {
            RemovedCloudDiscovery.check(RemovedCloudDiscovery.CLIENT_CLOUD_TAG);
        }
        isDiscoveryConfigurationConsistent(addressListProvided, kubernetesDiscoveryEnabled, discoverySpiEnabled);

        if (networkConfig.getAddresses().isEmpty() && discoveryService != null) {
            return new RemoteAddressProvider(() -> discoverAddresses(discoveryService), usePublicAddress(clientConfig));
        }
        TranslateToPublicAddressProvider toPublicAddressProvider = new TranslateToPublicAddressProvider(networkConfig,
                properties,
                loggingService.getLogger(TranslateToPublicAddressProvider.class));
        clusterService.addMembershipListener(toPublicAddressProvider);
        return new DefaultAddressProvider(networkConfig, toPublicAddressProvider);
    }

    private Map<Address, Address> discoverAddresses(DiscoveryService discoveryService) {
        Iterable<DiscoveryNode> discoveredNodes = checkNotNull(discoveryService.discoverNodes(),
                "Discovered nodes cannot be null!");
        Map<Address, Address> privateToPublic = new HashMap<>();
        for (DiscoveryNode discoveryNode : discoveredNodes) {
            privateToPublic.put(discoveryNode.getPrivateAddress(), discoveryNode.getPublicAddress());
        }
        return privateToPublic;
    }

    private boolean discoverySpiEnabled(ClientNetworkConfig networkConfig) {
        return (networkConfig.getDiscoveryConfig() != null && networkConfig.getDiscoveryConfig().isEnabled())
                || Boolean.parseBoolean(properties.getString(DISCOVERY_SPI_ENABLED));
    }

    private boolean usePublicAddress(ClientConfig config) {
        return properties.getBoolean(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED)
                || allUsePublicAddress(ClientAliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom(config.getNetworkConfig()));
    }

    private void isDiscoveryConfigurationConsistent(boolean addressListProvided, boolean kubernetesDiscoveryEnabled,
                                                    boolean discoverySpiEnabled) {
        List<String> enabledDiscoveries = new ArrayList<>();
        int count = 0;
        if (addressListProvided) {
            count++;
            enabledDiscoveries.add("cluster members given explicitly");
        }
        if (kubernetesDiscoveryEnabled) {
            count++;
            enabledDiscoveries.add("Kubernetes discovery");
        }
        if (discoverySpiEnabled) {
            count++;
            enabledDiscoveries.add("Discovery SPI");
        }
        if (count > 1) {
            throw new IllegalStateException("Only one discovery method can be enabled at a time. "
                    + "Keep only one of the following method enabled by removing the others from the configuration, "
                    + "or setting enabled to 'false': " + String.join(",", enabledDiscoveries));
        }
    }

    private DiscoveryService initDiscoveryService(ClientConfig config) {
        // Prevent confusing behavior where the DiscoveryService is started
        // and strategies are resolved but the AddressProvider is never registered
        List<DiscoveryStrategyConfig> aliasedDiscoveryConfigs =
                ClientAliasedDiscoveryConfigUtils.createDiscoveryStrategyConfigs(config);

        if (!properties.getBoolean(ClientProperty.DISCOVERY_SPI_ENABLED) && aliasedDiscoveryConfigs.isEmpty()
                && !config.getNetworkConfig().isAutoDetectionEnabled()) {
            return null;
        }

        ILogger logger = loggingService.getLogger(DiscoveryService.class);
        ClientNetworkConfig networkConfig = config.getNetworkConfig();
        DiscoveryConfig discoveryConfig = new DiscoveryConfigReadOnly(networkConfig.getDiscoveryConfig());

        DiscoveryServiceProvider factory = discoveryConfig.getDiscoveryServiceProvider();
        if (factory == null) {
            factory = new DefaultDiscoveryServiceProvider();
        }

        boolean isAutoDetectionEnabled = networkConfig.isAutoDetectionEnabled();
        DiscoveryServiceSettings settings = new DiscoveryServiceSettings()
                .setConfigClassLoader(config.getClassLoader())
                .setLogger(logger)
                .setDiscoveryMode(DiscoveryMode.Client)
                .setAliasedDiscoveryConfigs(aliasedDiscoveryConfigs)
                .setDiscoveryConfig(discoveryConfig)
                .setAutoDetectionEnabled(isAutoDetectionEnabled);

        DiscoveryService discoveryService = factory.newDiscoveryService(settings);
        if (isAutoDetectionEnabled && isEmptyDiscoveryStrategies(discoveryService)) {
            return null;
        }
        return discoveryService;
    }

    private boolean isEmptyDiscoveryStrategies(DiscoveryService discoveryService) {
        return discoveryService instanceof DefaultDiscoveryService dds
                && !dds.getDiscoveryStrategies().iterator().hasNext();
    }

    private ICredentialsFactory initCredentialsFactory(ClientConfig config) {
        ClientSecurityConfig securityConfig = config.getSecurityConfig();
        return securityConfig.asCredentialsFactory(config.getClassLoader());
    }

    private SocketInterceptor initSocketInterceptor(SocketInterceptorConfig sic) {
        if (sic != null && sic.isEnabled()) {
            return clientExtension.createSocketInterceptor(sic);
        }
        return null;
    }

}
