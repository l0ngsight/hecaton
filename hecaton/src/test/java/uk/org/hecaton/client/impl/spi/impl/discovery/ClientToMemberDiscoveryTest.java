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

package uk.org.hecaton.client.impl.spi.impl.discovery;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.XmlClientConfigBuilder;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.XmlConfigBuilder;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.discovery.multicast.MulticastDiscoveryStrategy;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.InputStream;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class ClientToMemberDiscoveryTest extends HecatonTestSupport {

    public final TestHecatonFactory factory = new TestHecatonFactory();

    Config serverConfig;
    ClientConfig clientConfig;

    @Parameter
    public Boolean safeSerialization;

    @Parameters(name = "{index}: safeSerialization: {0}")
    public static Object[] data() {
        return new Object[] { null, Boolean.FALSE, Boolean.TRUE };
    }

    @Before
    public void setup() {
        String serverXmlFileName = "hecaton-multicast-plugin-non-default-port.xml";
        String clientXmlFileName = "hecaton-client-multicast-plugin-non-default-port.xml";

        InputStream xmlResource = MulticastDiscoveryStrategy.class.getClassLoader().getResourceAsStream(serverXmlFileName);
        serverConfig = new XmlConfigBuilder(xmlResource).build();

        InputStream xmlClientResource = MulticastDiscoveryStrategy.class.getClassLoader()
                .getResourceAsStream(clientXmlFileName);
        clientConfig = new XmlClientConfigBuilder(xmlClientResource).build();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void clientTest() {
        if (safeSerialization != null) {
            serverConfig.getNetworkConfig().getJoin().getDiscoveryConfig().getDiscoveryStrategyConfigs().iterator().next()
                    .addProperty("safe-serialization", safeSerialization.toString());
            clientConfig.getNetworkConfig().getDiscoveryConfig().getDiscoveryStrategyConfigs().iterator().next()
                    .addProperty("safe-serialization", safeSerialization.toString());
        }
        factory.newHecatonInstance(serverConfig);
        factory.newHecatonInstance(serverConfig);

        HecatonInstance client = factory.newHecatonClient(clientConfig);
        assertClusterSizeEventually(2, client);
    }

    @Test
    public void clientSerializationMismatchTest() {
        if (safeSerialization == null) {
            return;
        }
        serverConfig.getNetworkConfig().getJoin().getDiscoveryConfig().getDiscoveryStrategyConfigs().iterator().next()
                .addProperty("safe-serialization", safeSerialization.toString());
        clientConfig.getNetworkConfig().setConnectionTimeout(15000).getDiscoveryConfig().getDiscoveryStrategyConfigs()
                .iterator().next().addProperty("safe-serialization", Boolean.toString(!safeSerialization));
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(3000);
        factory.newHecatonInstance(serverConfig);
        factory.newHecatonInstance(serverConfig);

        assertThrows(IllegalStateException.class, () -> factory.newHecatonClient(clientConfig));
    }
}
