/*
 * Copyright (c) 2026, The Hecaton Authors.
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
package uk.org.hecaton.wan.impl.batch;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.WanBatchPublisherConfig;
import uk.org.hecaton.internal.util.AddressUtil;
import uk.org.hecaton.internal.util.AddressUtil.AddressHolder;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.spi.discovery.DiscoveryNode;
import uk.org.hecaton.spi.discovery.integration.DiscoveryService;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The members of the target cluster a publisher sends to: the configured {@code target-endpoints},
 * or the nodes a discovery service finds (refreshed every {@code discovery-period-seconds}). The
 * publisher sends to one endpoint at a time and moves to the next when it fails, so the events
 * of a partition arrive in order.
 */
final class WanTargetEndpoints {

    private static final int DEFAULT_PORT = 5701;

    private final WanBatchPublisherConfig config;
    private final ILogger logger;
    /** Null unless the endpoints are discovered. */
    private final Supplier<DiscoveryService> discoveryService;
    private List<Address> endpoints = Collections.emptyList();
    private int current;
    private long nextDiscovery;

    WanTargetEndpoints(WanBatchPublisherConfig config, ILogger logger, Supplier<DiscoveryService> discoveryService) {
        this.config = config;
        this.logger = logger;
        this.discoveryService = discoveryService;
        if (discoveryService == null) {
            endpoints = parse(config.getTargetEndpoints());
        }
    }

    /** @return the endpoint to send to, or null if none is known */
    synchronized Address current() {
        if (discoveryService != null && System.currentTimeMillis() >= nextDiscovery) {
            discover();
        }
        return endpoints.isEmpty() ? null : endpoints.get(current % endpoints.size());
    }

    /** The endpoint failed: use the next one. */
    synchronized void failed(Address endpoint) {
        if (!endpoints.isEmpty() && endpoint.equals(endpoints.get(current % endpoints.size()))) {
            current = (current + 1) % endpoints.size();
        }
        if (discoveryService != null) {
            // it may have left the target cluster
            nextDiscovery = 0;
        }
    }

    private void discover() {
        nextDiscovery = System.currentTimeMillis() + SECONDS.toMillis(config.getDiscoveryPeriodSeconds());
        List<Address> discovered = new ArrayList<>();
        try {
            for (DiscoveryNode node : discoveryService.get().discoverNodes()) {
                Address address = config.isUseEndpointPrivateAddress() || node.getPublicAddress() == null
                        ? node.getPrivateAddress() : node.getPublicAddress();
                if (address != null && discovered.size() < config.getMaxTargetEndpoints()) {
                    discovered.add(address);
                }
            }
        } catch (RuntimeException e) {
            logger.warning("Could not discover the members of WAN target cluster " + config.getClusterName(), e);
            return;
        }
        if (!discovered.equals(endpoints)) {
            logger.info("WAN target cluster " + config.getClusterName() + " endpoints: " + discovered);
            Address previous = endpoints.isEmpty() ? null : endpoints.get(current % endpoints.size());
            endpoints = discovered;
            current = Math.max(0, previous == null ? 0 : discovered.indexOf(previous));
        }
    }

    private List<Address> parse(String targetEndpoints) {
        List<Address> parsed = new ArrayList<>();
        if (targetEndpoints == null) {
            return parsed;
        }
        for (String endpoint : targetEndpoints.split(",")) {
            String trimmed = endpoint.trim();
            if (trimmed.isEmpty() || parsed.size() >= config.getMaxTargetEndpoints()) {
                continue;
            }
            AddressHolder holder = AddressUtil.getAddressHolder(trimmed, DEFAULT_PORT);
            try {
                parsed.add(new Address(holder.getAddress(), holder.getPort()));
            } catch (UnknownHostException e) {
                logger.warning("Unknown WAN target endpoint " + trimmed + " of cluster " + config.getClusterName());
            }
        }
        return parsed;
    }

    /** The number of known endpoints. */
    int count() {
        return endpoints.size();
    }

}
