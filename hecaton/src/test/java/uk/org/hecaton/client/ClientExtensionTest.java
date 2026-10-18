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

package uk.org.hecaton.client;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.RoutingStrategy;
import uk.org.hecaton.client.impl.ClientExtension;
import uk.org.hecaton.client.impl.clientside.ClientConnectionManagerFactory;
import uk.org.hecaton.client.impl.clientside.DefaultClientConnectionManagerFactory;
import uk.org.hecaton.client.impl.clientside.DefaultClientExtension;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.impl.spi.ClientProxyFactory;
import uk.org.hecaton.client.impl.spi.impl.ClientClusterServiceImpl;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientExtensionTest extends HecatonTestSupport {
    private final ClientConnectionManagerFactory factory = new DefaultClientConnectionManagerFactory();
    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @Test
    public void test_createServiceProxyFactory() {
        ClientExtension clientExtension = new DefaultClientExtension();
        assertInstanceOf(ClientProxyFactory.class, clientExtension.createServiceProxyFactory(MapService.class));
    }

    @Test
    public void test_createServiceProxyFactory_whenUnknownServicePassed() {
        ClientExtension clientExtension = new DefaultClientExtension();
        assertThatThrownBy(() -> clientExtension.createServiceProxyFactory(TestService.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Proxy factory cannot be created. Unknown service");
    }

    @Test
    public void test_createClientClusterServiceWithMultiMemberRouting_throwsInvalidConfigurationException() throws IOException {
        String expectedMsg = "MULTI_MEMBER routing is an enterprise feature since 5.5. "
                + "You must use Hecaton enterprise to enable this feature.";
        try (var classLoader = new URLClassLoader(new URL[0])) {
            ClientConfig clientConfig = new ClientConfig();
            // avoid loading anything but DefaultClientExtension
            clientConfig.setClassLoader(classLoader);
            clientConfig.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.MULTI_MEMBER)
                        .setRoutingStrategy(RoutingStrategy.PARTITION_GROUPS);
            var addressProvider = hecatonFactory.createAddressProvider(clientConfig);

            assertThatThrownBy(() -> new HecatonClientInstanceImpl(UUID.randomUUID().toString(),
                                                                     clientConfig, null,
                                                                     factory, addressProvider))
                    .isInstanceOf(InvalidConfigurationException.class)
                    .hasMessageContaining(expectedMsg);
        }
    }

    @Test
    public void test_createClientClusterService() throws IOException {
        ClientExtension clientExtension = new DefaultClientExtension();
        ClientConfig clientConfig = new ClientConfig();
        var addressProvider = hecatonFactory.createAddressProvider(clientConfig);

        try (var classLoader = new URLClassLoader(new URL[0])) {
            // avoid loading anything but DefaultClientExtension
            clientConfig.setClassLoader(classLoader);
            var client = new HecatonClientInstanceImpl(UUID.randomUUID().toString(),
                                                         clientConfig, null,
                                                         factory, addressProvider);
            assertInstanceOf(ClientClusterServiceImpl.class,
                             clientExtension.createClientClusterService(client));
        }
    }

    private static class TestService {

    }
}
