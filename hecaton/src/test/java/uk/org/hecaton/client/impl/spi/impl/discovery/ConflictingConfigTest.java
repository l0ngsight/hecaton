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
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConflictingConfigTest {


    @Test(expected = IllegalStateException.class)
    public void testHecatonCloud_and_DiscoverySPIEnabled() {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().getCloudConfig().setEnabled(true);
        config.setProperty(ClientProperty.DISCOVERY_SPI_ENABLED.getName(), "true");
        HecatonClient.newHecatonClient(config);
    }

    @Test(expected = IllegalStateException.class)
    public void testHecatonCloudViaProperty_and_DiscoverySPIEnabled() {
        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperty.DISCOVERY_SPI_ENABLED.getName(), "true");
        config.setProperty(ClientProperty.HECATON_CLOUD_DISCOVERY_TOKEN.getName(), "TOKEN");
        HecatonClient.newHecatonClient(config);
    }

    @Test(expected = IllegalStateException.class)
    public void testHecatonCloud_firstClass_and_propertyBased() {
        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperty.HECATON_CLOUD_DISCOVERY_TOKEN.getName(), "TOKEN");
        config.getNetworkConfig().getCloudConfig().setEnabled(true);
        HecatonClient.newHecatonClient(config);
    }

    @Test(expected = IllegalStateException.class)
    public void testClusterMembersGiven_and_DiscoverySPIEnabled() {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1");
        config.setProperty(ClientProperty.DISCOVERY_SPI_ENABLED.getName(), "true");
        HecatonClient.newHecatonClient(config);
    }

    @Test(expected = IllegalStateException.class)
    public void testClusterMembersGiven_and_HecatonCloudEnabled() {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1");
        config.getNetworkConfig().getCloudConfig().setEnabled(true);
        HecatonClient.newHecatonClient(config);
    }

    @Test(expected = IllegalStateException.class)
    public void testClusterMembersGiven_and_HecatonCloudViaProperty() {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1");
        config.setProperty(ClientProperty.HECATON_CLOUD_DISCOVERY_TOKEN.getName(), "TOKEN");
        HecatonClient.newHecatonClient(config);
    }
}
