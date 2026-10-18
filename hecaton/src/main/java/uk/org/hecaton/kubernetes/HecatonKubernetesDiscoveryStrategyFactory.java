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

package uk.org.hecaton.kubernetes;

import uk.org.hecaton.config.properties.PropertyDefinition;
import uk.org.hecaton.instance.impl.ClusterTopologyIntentTracker;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.spi.discovery.DiscoveryNode;
import uk.org.hecaton.spi.discovery.DiscoveryStrategy;
import uk.org.hecaton.spi.discovery.DiscoveryStrategyFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static uk.org.hecaton.instance.impl.Node.DISCOVERY_PROPERTY_CLUSTER_TOPOLOGY_INTENT_TRACKER;

/**
 * Just the factory to create the Kubernetes Discovery Strategy
 */
public class HecatonKubernetesDiscoveryStrategyFactory
        implements DiscoveryStrategyFactory {
    private static final Collection<PropertyDefinition> PROPERTY_DEFINITIONS;

    static {
        PROPERTY_DEFINITIONS = Collections.unmodifiableCollection(Arrays.asList(
                KubernetesProperties.SERVICE_DNS,
                KubernetesProperties.SERVICE_DNS_TIMEOUT,
                KubernetesProperties.SERVICE_NAME,
                KubernetesProperties.SERVICE_LABEL_NAME,
                KubernetesProperties.SERVICE_LABEL_VALUE,
                KubernetesProperties.NAMESPACE,
                KubernetesProperties.POD_LABEL_NAME,
                KubernetesProperties.POD_LABEL_VALUE,
                KubernetesProperties.RESOLVE_NOT_READY_ADDRESSES,
                KubernetesProperties.USE_NODE_NAME_AS_EXTERNAL_ADDRESS,
                KubernetesProperties.EXPOSE_EXTERNALLY,
                KubernetesProperties.SERVICE_PER_POD_LABEL_NAME,
                KubernetesProperties.SERVICE_PER_POD_LABEL_VALUE,
                KubernetesProperties.KUBERNETES_API_RETIRES,
                KubernetesProperties.KUBERNETES_MASTER_URL,
                KubernetesProperties.KUBERNETES_API_TOKEN,
                KubernetesProperties.KUBERNETES_CA_CERTIFICATE,
                KubernetesProperties.SERVICE_PORT));
    }

    private final String tokenPath;

    public HecatonKubernetesDiscoveryStrategyFactory() {
        this("/var/run/secrets/kubernetes.io/serviceaccount/token");
    }

    /**
     * Used externally only for testing
     */
    HecatonKubernetesDiscoveryStrategyFactory(String tokenPath) {
        this.tokenPath = tokenPath;
    }

    @Override
    public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
        return HecatonKubernetesDiscoveryStrategy.class;
    }

    @Override
    public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger,
                                                  Map<String, Comparable> properties) {
        ClusterTopologyIntentTracker tracker = null;
        if (discoveryNode != null) {
            tracker = (ClusterTopologyIntentTracker) discoveryNode.getProperties()
                    .get(DISCOVERY_PROPERTY_CLUSTER_TOPOLOGY_INTENT_TRACKER);
        }
        return new HecatonKubernetesDiscoveryStrategy(logger, properties, tracker);
    }

    @Override
    public Collection<PropertyDefinition> getConfigurationProperties() {
        return PROPERTY_DEFINITIONS;
    }

    /**
     * In all Kubernetes environments the file "/var/run/secrets/kubernetes.io/serviceaccount/token" is injected into the
     * container. That is why we can use it to verify if this code is run in the Kubernetes environment.
     * <p>
     * Note that if the Kubernetes environment is not configured correctly, this file my not exist. However, in such case,
     * this plugin won't work anyway, so it makes perfect sense to return {@code false}.
     *
     * @return true if running in the Kubernetes environment
     */
    @Override
    public boolean isAutoDetectionApplicable() {
        return tokenFileExists() && defaultKubernetesMasterReachable();
    }

    boolean tokenFileExists() {
        return new File(tokenPath).exists();
    }

    private boolean defaultKubernetesMasterReachable() {
        try {
            InetAddress.getByName("kubernetes.default.svc");
            return true;
        } catch (UnknownHostException e) {
            ILogger logger = Logger.getLogger(HecatonKubernetesDiscoveryStrategyFactory.class);
            logger.warning("Hecaton running on Kubernetes, but \"kubernetes.default.svc\" is not reachable. "
                    + "Check your Kubernetes DNS configuration.");
            logger.finest(e);
            return false;
        }
    }

    @Override
    public DiscoveryStrategyLevel discoveryStrategyLevel() {
        return DiscoveryStrategyLevel.PLATFORM;
    }
}
