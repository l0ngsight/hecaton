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

package uk.org.hecaton.client.test;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.HecatonClientUtil;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.XmlClientConfigBuilder;
import uk.org.hecaton.client.config.impl.ClientAliasedDiscoveryConfigUtils;
import uk.org.hecaton.client.impl.clientside.ClientConnectionManagerFactory;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.clientside.HecatonClientProxy;
import uk.org.hecaton.client.impl.connection.AddressProvider;
import uk.org.hecaton.client.impl.connection.Addresses;
import uk.org.hecaton.client.impl.management.ClientConnectionProcessListenerRegistry;
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.client.util.AddressHelper;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.DiscoveryStrategyConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.metrics.impl.MetricsRegistryImpl;
import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.test.TestEnvironment;
import uk.org.hecaton.test.TestHecatonInstanceFactory;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class TestHecatonFactory extends TestHecatonInstanceFactory {

    public static final String TEST_JVM_PREFIX = "test-jvm-";
    private static final String jvmName = ManagementFactory.getRuntimeMXBean().getName();
    private final boolean mockNetwork = TestEnvironment.isMockNetwork();
    private final ConcurrentMap<String, HecatonClientInstanceImpl> clients = new ConcurrentHashMap<>(10);
    private final TestClientRegistry clientRegistry = new TestClientRegistry(getRegistry());

    public TestHecatonFactory(int initialPort, String... addresses) {
        super(initialPort, addresses);
    }

    public TestHecatonFactory(int count) {
        super(count);
    }

    public TestHecatonFactory() {
        this(0);
    }

    public HecatonInstance newHecatonClient() {
        return newHecatonClient(null);
    }

    public HecatonInstance newHecatonClient(ClientConfig config) {
        return newHecatonClient(config, null);
    }

    public HecatonInstance newHecatonClient(ClientConfig config, String sourceIp) {
        if (!mockNetwork) {
            HecatonInstance client = HecatonClient.newHecatonClient(config);
            registerJvmNameAndPidMetric(((HecatonClientProxy) client).client);
            return client;
        }

        if (config == null) {
            config = new XmlClientConfigBuilder().build();
        }

        ClientConnectionManagerFactory connectionManagerFactory = clientRegistry.createClientServiceFactory(sourceIp);
        AddressProvider addressProvider = createAddressProvider(config);
        HecatonInstance proxy = HecatonClientUtil.newHecatonClient(config, connectionManagerFactory, addressProvider);
        HecatonClientInstanceImpl client = ((HecatonClientProxy) proxy).client;
        registerJvmNameAndPidMetric(client);
        clients.put(client.getName(), client);
        return proxy;
    }

    private void registerJvmNameAndPidMetric(HecatonClientInstanceImpl client) {
        int pid = Integer.parseInt(jvmName.substring(0, jvmName.indexOf("@")));
        MetricsRegistryImpl metricsRegistry = client.getMetricsRegistry();
        metricsRegistry.registerDynamicMetricsProvider(
                (descriptor, context) -> context
                        .collect(descriptor.withPrefix(TEST_JVM_PREFIX + jvmName).withMetric("pid"), pid));
    }

    // used by MC tests
    public HecatonInstance getHecatonClientByName(String clientName) {
        return clients.get(clientName);
    }

    public AddressProvider createAddressProvider(ClientConfig config) {
        boolean discoveryEnabled = new HecatonProperties(config.getProperties())
                .getBoolean(ClientProperty.DISCOVERY_SPI_ENABLED);

        List<DiscoveryStrategyConfig> aliasedDiscoveryConfigs =
                ClientAliasedDiscoveryConfigUtils.createDiscoveryStrategyConfigs(config);

        List<String> userConfiguredAddresses = config.getNetworkConfig().getAddresses();

        boolean isAtLeastAProviderConfigured = discoveryEnabled || !aliasedDiscoveryConfigs.isEmpty()
                || !userConfiguredAddresses.isEmpty();

        if (isAtLeastAProviderConfigured) {
            // address providers or addresses are configured explicitly, don't add more addresses
            return null;
        }

        return new AddressProvider() {
            @Override
            public Addresses loadAddresses(ClientConnectionProcessListenerRegistry listenerRunner) {
                Addresses possibleAddresses = new Addresses();
                for (Address address : getKnownAddresses()) {
                    Addresses addresses = AddressHelper.getPossibleSocketAddresses(address.getPort(),
                            address.getHost(), 1, listenerRunner);
                    possibleAddresses.addAll(addresses);
                }
                return possibleAddresses;
            }

            @Override
            public Address translate(Address address) {
                return address;
            }

            @Override
            public Address translate(Member member) {
                return member.getAddress();
            }
        };
    }

    public void shutdownAllMembers() {
        super.shutdownAll();
    }

    @Override
    public void shutdownAll() {
        if (mockNetwork) {
            for (HecatonClientInstanceImpl client : clients.values()) {
                client.shutdown();
            }
        } else {
            // for client terminateAll() and shutdownAll() is the same
            HecatonClient.shutdownAll();
        }
        super.shutdownAll();
    }

    @Override
    public void terminateAll() {
        if (mockNetwork) {
            for (HecatonClientInstanceImpl client : clients.values()) {
                client.getLifecycleService().terminate();
            }
        } else {
            // for client terminateAll() and shutdownAll() is the same
            HecatonClient.shutdownAll();
        }
        super.terminateAll();
    }
}
