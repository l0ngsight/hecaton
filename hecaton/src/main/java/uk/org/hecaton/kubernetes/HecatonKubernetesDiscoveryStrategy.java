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

import uk.org.hecaton.instance.impl.ClusterTopologyIntentTracker;
import uk.org.hecaton.kubernetes.KubernetesConfig.DiscoveryMode;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.discovery.AbstractDiscoveryStrategy;
import uk.org.hecaton.spi.discovery.DiscoveryNode;
import uk.org.hecaton.spi.partitiongroup.PartitionGroupMetaData;
import uk.org.hecaton.spi.utils.RetryUtils;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class HecatonKubernetesDiscoveryStrategy
        extends AbstractDiscoveryStrategy {
    private static final int DNS_RETRY = 5;

    private final EndpointResolver endpointResolver;
    private final Map<String, String> memberMetadata = new HashMap<>();

    HecatonKubernetesDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties,
                                         ClusterTopologyIntentTracker clusterTopologyIntentTracker) {
        super(logger, properties);

        KubernetesConfig config = new KubernetesConfig(properties);
        logger.info(config.toString());

        if (DiscoveryMode.DNS_LOOKUP == config.getMode()) {
            endpointResolver = new DnsEndpointResolver(logger, config);
        } else {
            endpointResolver = new KubernetesApiEndpointResolver(logger, config, clusterTopologyIntentTracker);
        }

        logger.info("Kubernetes Discovery activated with mode: " + config.getMode().name());
    }

    @Override
    public void start() {
        endpointResolver.start();
    }

    @Override
    public Map<String, String> discoverLocalMetadata() {
        if (memberMetadata.isEmpty()) {
            memberMetadata.put(PartitionGroupMetaData.PARTITION_GROUP_ZONE, endpointResolver.resolveCurrentZone());
            memberMetadata.put("hecaton.partition.group.node", endpointResolver.resolveCurrentNodeName());
        }
        return memberMetadata;
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        return endpointResolver.resolveNodes();
    }

    @Override
    public void destroy() {
        endpointResolver.destroy();
    }

    abstract static class EndpointResolver {
        protected final ILogger logger;

        EndpointResolver(ILogger logger) {
            this.logger = logger;
        }

        abstract List<DiscoveryNode> resolveNodes();

        /**
         * Discovers the availability zone in which the current Hecaton member is running.
         * <p>
         * Note: ZONE_AWARE is available only for the Kubernetes API Mode.
         */
        String resolveCurrentZone() {
            return "unknown";
        }

        /**
         * Discovers the name of the node which the current Hecaton member pod is running on.
         * <p>
         * Note: NODE_AWARE is available only for the Kubernetes API Mode.
         */
        String resolveCurrentNodeName() {
            return "unknown";
        }

        void start() {
        }

        void destroy() {
        }

        protected InetAddress mapAddress(String address) {
            if (address == null) {
                return null;
            }
            try {
                return RetryUtils.retry(() -> InetAddress.getByName(address), DNS_RETRY);
            } catch (Exception e) {
                logger.warning("Address '" + address + "' could not be resolved");
            }
            return null;
        }
    }
}
