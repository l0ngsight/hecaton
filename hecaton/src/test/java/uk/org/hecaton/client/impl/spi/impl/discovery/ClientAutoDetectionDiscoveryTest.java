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

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.AbstractHecatonClassRunner.getTestMethodName;
import static uk.org.hecaton.test.TestEnvironment.isSolaris;

@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientAutoDetectionDiscoveryTest extends HecatonTestSupport {

    @After
    public void tearDown() {
        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
        HecatonClient.shutdownAll();
    }

    @Test
    public void defaultDiscovery() {
        Config c = new Config();
        c.setClusterName(getTestMethodName());

        if (isSolaris()) {
            c.setProperty(ClusterProperty.MULTICAST_SOCKET_SET_INTERFACE.getName(), "false");
        }

        Hecaton.newHecatonInstance(c);
        Hecaton.newHecatonInstance(c);

        ClientConfig clientConfig = new ClientConfig().setClusterName(getTestMethodName());
        HecatonInstance client = HecatonClient.newHecatonClient(clientConfig);
        assertClusterSizeEventually(2, client);
    }

    @Test
    public void autoDetectionDisabled() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        Hecaton.newHecatonInstance(config);
        Hecaton.newHecatonInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().getAutoDetectionConfig().setEnabled(false);
        HecatonInstance client = HecatonClient.newHecatonClient(clientConfig);
        // uses 127.0.0.1 and finds only one standalone member
        assertClusterSizeEventually(1, client);
    }

    @Test
    public void autoDetectionNotUsedWhenOtherDiscoveryEnabled() {
        Config config = new Config();
        config.getNetworkConfig().setPort(5710);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        Hecaton.newHecatonInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:5710");
        HecatonInstance client = HecatonClient.newHecatonClient(clientConfig);
        assertClusterSizeEventually(1, client);
    }
}
