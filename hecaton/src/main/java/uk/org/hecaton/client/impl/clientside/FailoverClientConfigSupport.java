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

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientFailoverConfig;
import uk.org.hecaton.client.config.ClientNetworkConfig;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.core.HecatonException;

import java.util.List;

/**
 * Static methods to resolve and validate multiple client configs for blue green feature
 */
public final class FailoverClientConfigSupport {

    private FailoverClientConfigSupport() {
    }

    /**
     * Loads failover configuration using the search logic described in
     * {@link HecatonClient#newHecatonFailoverClient()} and builds
     * {@link ClientFailoverConfig}.
     *
     * @return resolvedConfigs
     * @throws HecatonException            if no failover configuration is found
     * @throws InvalidConfigurationException when given config is not valid
     */
    public static ClientFailoverConfig resolveClientFailoverConfig() {
        return resolveClientFailoverConfig(null);
    }

    /**
     * If {@code clientFailoverConfig} passed {@code null}, then loads
     * failover configuration using the search logic described in
     * {@link HecatonClient#newHecatonFailoverClient()} and builds
     * {@link ClientFailoverConfig}.
     *
     * @param clientFailoverConfig provided via {@link HecatonClient#newHecatonFailoverClient(ClientFailoverConfig)}
     * @return resolvedConfigs
     * @throws HecatonException            if {@code clientFailoverConfig} is {@code null} and
     *                                       no failover configuration is found
     * @throws InvalidConfigurationException when given config is not valid
     */
    public static ClientFailoverConfig resolveClientFailoverConfig(ClientFailoverConfig clientFailoverConfig) {
        if (clientFailoverConfig == null) {
            clientFailoverConfig = ClientFailoverConfig.load();
        }
        checkValidAlternative(clientFailoverConfig.getClientConfigs());
        return clientFailoverConfig;
    }

    /**
     * If clientConfig is {@code null}, the client configuration is loaded
     * using the resolution logic as described in {@link HecatonClient#newHecatonClient()}
     * <p/>
     * Used with
     * {@link HecatonClient#newHecatonClient} or
     * {@link HecatonClient#newHecatonClient(ClientConfig)}
     *
     * @param config provided via {@link HecatonClient#newHecatonClient(ClientConfig)}
     * @return resolvedConfigs
     * @throws InvalidConfigurationException when given config is not valid
     */
    public static ClientConfig resolveClientConfig(ClientConfig config) {
        if (config == null) {
            return ClientConfig.load();
        }
        return config;
    }

    /**
     * For a client to be valid alternative, all configurations must be equal except
     * Cluster name
     * SecurityConfig
     * Discovery related parts of NetworkConfig
     * Credentials related configs
     *
     * @param alternativeClientConfigs to check if they are valid alternative for a single client two switch between clusters
     * @throws InvalidConfigurationException when given configs are not valid
     */
    private static void checkValidAlternative(List<ClientConfig> alternativeClientConfigs) {
        if (alternativeClientConfigs.isEmpty()) {
            throw new InvalidConfigurationException("ClientFailoverConfig must have at least one client config.");
        }
        ClientConfig mainConfig = alternativeClientConfigs.get(0);
        for (ClientConfig alternativeClientConfig : alternativeClientConfigs.subList(1, alternativeClientConfigs.size())) {
            checkValidAlternative(mainConfig, alternativeClientConfig);
        }
    }

    private static void throwInvalidConfigurationException(String rootClusterName, String clusterName, String configName) {
        throw new InvalidConfigurationException("Alternative config with cluster name " + clusterName
                + " has a different config than the initial config with cluster name " + rootClusterName + " for " + configName);
    }

    /**
     * @return false when both objects are null or they are equal, true otherwise
     */
    private static boolean notEqual(Object l, Object r) {
        return l != null ? !l.equals(r) : r != null;
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity", "checkstyle:methodlength"})
    private static void checkValidAlternative(ClientConfig mainConfig, ClientConfig alternativeConfig) {
        String mainClusterName = mainConfig.getClusterName();
        String alternativeClusterName = alternativeConfig.getClusterName();

        checkValidAlternativeForNetwork(mainConfig, alternativeConfig);

        if (notEqual(mainConfig.getProperties(), alternativeConfig.getProperties())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "properties");
        }
        if (notEqual(mainConfig.getLoadBalancer(), alternativeConfig.getLoadBalancer())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "loadBalancer");
        }
        if (notEqual(mainConfig.getLoadBalancerClassName(), alternativeConfig.getLoadBalancerClassName())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "loadBalancerClassName");
        }
        if (notEqual(mainConfig.getListenerConfigs(), alternativeConfig.getListenerConfigs())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "listeners");
        }
        if (notEqual(mainConfig.getInstanceName(), alternativeConfig.getInstanceName())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "instanceName");
        }
        if (notEqual(mainConfig.getConfigPatternMatcher(), alternativeConfig.getConfigPatternMatcher())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "configPatternMatcher");
        }
        if (notEqual(mainConfig.getNearCacheConfigMap(), alternativeConfig.getNearCacheConfigMap())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "nearCache");
        }
        if (notEqual(mainConfig.getReliableTopicConfigMap(), alternativeConfig.getReliableTopicConfigMap())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "reliableTopic");
        }
        if (notEqual(mainConfig.getQueryCacheConfigs(), alternativeConfig.getQueryCacheConfigs())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "queryCacheConfigs");
        }
        if (notEqual(mainConfig.getSerializationConfig(), alternativeConfig.getSerializationConfig())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "serializationConfig");
        }
        if (notEqual(mainConfig.getNativeMemoryConfig(), alternativeConfig.getNativeMemoryConfig())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "nativeMemory");
        }
        if (notEqual(mainConfig.getProxyFactoryConfigs(), alternativeConfig.getProxyFactoryConfigs())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "proxyFactory");
        }
        if (notEqual(mainConfig.getManagedContext(), alternativeConfig.getManagedContext())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "managedContext");
        }
        if (notEqual(mainConfig.getClassLoader(), alternativeConfig.getClassLoader())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "classLoader");
        }
        if (notEqual(mainConfig.getConnectionStrategyConfig(), alternativeConfig.getConnectionStrategyConfig())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "connectionStrategy");
        }
        if (notEqual(mainConfig.getUserCodeDeploymentConfig(), alternativeConfig.getUserCodeDeploymentConfig())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "userCodeDeployment");
        }
        if (notEqual(mainConfig.getFlakeIdGeneratorConfigMap(), alternativeConfig.getFlakeIdGeneratorConfigMap())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "flakeIdGenerator");
        }
        if (notEqual(mainConfig.getLabels(), alternativeConfig.getLabels())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "labels");
        }
        if (notEqual(mainConfig.getUserContext(), alternativeConfig.getUserContext())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "userContext");
        }
        if (notEqual(mainConfig.getMetricsConfig(), alternativeConfig.getMetricsConfig())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "metricsConfig");
        }
        if (notEqual(mainConfig.getInstanceTrackingConfig(), alternativeConfig.getInstanceTrackingConfig())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "instanceTrackingConfig");
        }
        if (mainConfig.isBackupAckToClientEnabled() != alternativeConfig.isBackupAckToClientEnabled()) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "isBackupAckToClientEnabled");
        }
        if (notEqual(mainConfig.getSqlConfig(), alternativeConfig.getSqlConfig())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "sqlConfig");
        }
        if (notEqual(mainConfig.getTpcConfig(), alternativeConfig.getTpcConfig())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "tpcConfig");
        }
        if (notEqual(mainConfig.isCPDirectToLeaderRoutingEnabled(), alternativeConfig.isCPDirectToLeaderRoutingEnabled())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "isCPDirectToLeaderRoutingEnabled");
        }
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity", "checkstyle:methodlength"})
    private static void checkValidAlternativeForNetwork(ClientConfig mainConfig, ClientConfig alternativeConfig) {
        String mainClusterName = mainConfig.getClusterName();
        String alternativeClusterName = alternativeConfig.getClusterName();

        ClientNetworkConfig mainNetworkConfig = mainConfig.getNetworkConfig();
        ClientNetworkConfig alternativeNetworkConfig = alternativeConfig.getNetworkConfig();

        if (mainNetworkConfig == null && alternativeNetworkConfig == null) {
            return;
        }

        if (mainNetworkConfig == null || alternativeNetworkConfig == null) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "network");
        }

        if (mainNetworkConfig.getClusterRoutingConfig().getRoutingMode()
                != alternativeNetworkConfig.getClusterRoutingConfig().getRoutingMode()) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "network:clusterRouting:mode");
        }
        if (mainNetworkConfig.isRedoOperation() != alternativeNetworkConfig.isRedoOperation()) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "network:redoOperation");
        }
        if (mainNetworkConfig.getConnectionTimeout() != alternativeNetworkConfig.getConnectionTimeout()) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "network:connectionTimeout");
        }
        if (notEqual(mainNetworkConfig.getSocketOptions(), alternativeNetworkConfig.getSocketOptions())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "network:socketOptions");
        }
        if (notEqual(mainNetworkConfig.getOutboundPortDefinitions(), alternativeNetworkConfig.getOutboundPortDefinitions())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "network:outboundPortDefinitions");
        }
        if (notEqual(mainNetworkConfig.getOutboundPorts(), alternativeNetworkConfig.getOutboundPorts())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "network:outboundPorts");
        }
        if (notEqual(mainNetworkConfig.getClientIcmpPingConfig(), alternativeNetworkConfig.getClientIcmpPingConfig())) {
            throwInvalidConfigurationException(mainClusterName, alternativeClusterName, "network:clientIcmp");
        }
    }
}
