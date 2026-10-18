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

package uk.org.hecaton.spi.discovery.multicast;

import uk.org.hecaton.config.properties.ValidationException;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.spi.discovery.AbstractDiscoveryStrategy;
import uk.org.hecaton.spi.discovery.DiscoveryNode;
import uk.org.hecaton.spi.discovery.SimpleDiscoveryNode;
import uk.org.hecaton.spi.discovery.multicast.impl.MulticastDiscoveryReceiver;
import uk.org.hecaton.spi.discovery.multicast.impl.MulticastDiscoverySender;
import uk.org.hecaton.spi.discovery.multicast.impl.MulticastDiscoverySerializationHelper;
import uk.org.hecaton.spi.discovery.multicast.impl.MulticastMemberInfo;
import uk.org.hecaton.spi.partitiongroup.PartitionGroupStrategy;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;

import static uk.org.hecaton.internal.util.ExceptionUtil.rethrow;

/**
 * The multicast {@link uk.org.hecaton.spi.discovery.DiscoveryStrategy}.
 */
public class MulticastDiscoveryStrategy extends AbstractDiscoveryStrategy {

    private static final int DATA_OUTPUT_BUFFER_SIZE = 64 * 1024;
    private static final int DEFAULT_MULTICAST_PORT = 54327;
    private static final int SOCKET_TIME_TO_LIVE = 255;
    private static final int SOCKET_TIMEOUT = 3000;
    private static final String DEFAULT_MULTICAST_GROUP = "224.2.2.3";
    private static final Boolean DEFAULT_SAFE_SERIALIZATION = Boolean.FALSE;

    private final DiscoveryNode discoveryNode;
    private final ILogger logger;
    private Thread thread;
    private MulticastDiscoveryReceiver multicastDiscoveryReceiver;
    private MulticastDiscoverySender multicastDiscoverySender;
    private boolean isClient;

    public MulticastDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger, Map<String, Comparable> properties) {
        super(logger, properties);
        this.discoveryNode = discoveryNode;
        this.logger = logger;
    }

    private void initializeMulticastSocket() {
        try {
            int port = getOrDefault(MulticastProperties.PORT, DEFAULT_MULTICAST_PORT);
            PortValueValidator.validate(port);
            String group = getOrDefault(MulticastProperties.GROUP, DEFAULT_MULTICAST_GROUP);
            boolean safeSerialization = getOrDefault(MulticastProperties.SAFE_SERIALIZATION, DEFAULT_SAFE_SERIALIZATION);
            if (!safeSerialization) {
                String prop = MulticastProperties.SAFE_SERIALIZATION.key();
                logger.warning("The " + getClass().getSimpleName()
                        + " Hecaton member discovery strategy is configured without the " + prop + " parameter enabled."
                        + " Set the " + prop + " property to 'true' in the strategy configuration to protect the cluster"
                        + " against untrusted deserialization attacks.");
            }
            MulticastDiscoverySerializationHelper serializationHelper = new MulticastDiscoverySerializationHelper(
                    safeSerialization);
            MulticastSocket multicastSocket = new MulticastSocket(null);
            multicastSocket.setReuseAddress(true);
            multicastSocket.bind(new InetSocketAddress(port));
            if (discoveryNode != null) {
                // See MulticastService.createMulticastService(...)
                InetAddress inetAddress = discoveryNode.getPrivateAddress().getInetAddress();
                if (!inetAddress.isLoopbackAddress()) {
                    multicastSocket.setInterface(inetAddress);
                }
            }
            multicastSocket.setTimeToLive(SOCKET_TIME_TO_LIVE);
            multicastSocket.setReceiveBufferSize(DATA_OUTPUT_BUFFER_SIZE);
            multicastSocket.setSendBufferSize(DATA_OUTPUT_BUFFER_SIZE);
            multicastSocket.setSoTimeout(SOCKET_TIMEOUT);
            multicastSocket.joinGroup(InetAddress.getByName(group));
            multicastDiscoverySender = new MulticastDiscoverySender(discoveryNode, multicastSocket, logger, group, port,
                    serializationHelper);
            multicastDiscoveryReceiver = new MulticastDiscoveryReceiver(multicastSocket, logger, serializationHelper);
            if (discoveryNode == null) {
                isClient = true;
            }
        } catch (Exception e) {
            logger.finest(e.getMessage());
            throw rethrow(e);
        }
    }

    @Override
    public void start() {
        initializeMulticastSocket();
        if (!isClient) {
            thread = new Thread(multicastDiscoverySender);
            thread.start();
        }
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        DiscoveryNode discoveryNode;
        MulticastMemberInfo multicastMemberInfo = multicastDiscoveryReceiver.receive();

        if (multicastMemberInfo == null) {
            return null;
        }
        ArrayList<DiscoveryNode> arrayList = new ArrayList<>();
        try {
            discoveryNode = new SimpleDiscoveryNode(new Address(multicastMemberInfo.getHost(), multicastMemberInfo.getPort()));
            arrayList.add(discoveryNode);
        } catch (UnknownHostException e) {
            logger.finest(e.getMessage());
        }
        return arrayList;
    }

    @Override
    public void destroy() {
        multicastDiscoverySender.stop();
        if (thread != null) {
            thread.interrupt();
        }
    }

    @Override
    public PartitionGroupStrategy getPartitionGroupStrategy() {
        return null;
    }

    /**
     * Validator for valid network ports
     */
    private static class PortValueValidator {
        private static final int MIN_PORT = 0;
        private static final int MAX_PORT = 65535;

        public static void validate(int value) throws ValidationException {
            if (value < MIN_PORT) {
                throw new ValidationException("hz-port number must be greater 0");
            }
            if (value > MAX_PORT) {
                throw new ValidationException("hz-port number must be less or equal to 65535");
            }
        }
    }
}
