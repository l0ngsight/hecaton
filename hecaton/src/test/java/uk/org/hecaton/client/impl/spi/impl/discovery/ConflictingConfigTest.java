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
import uk.org.hecaton.config.InvalidConfigurationException;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConflictingConfigTest {

    @Test(expected = IllegalStateException.class)
    public void testClusterMembersGiven_and_DiscoverySPIEnabled() {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress("127.0.0.1");
        config.setProperty(ClientProperty.DISCOVERY_SPI_ENABLED.getName(), "true");
        HecatonClient.newHecatonClient(config);
    }

    @Test
    public void testRemovedHazelcastCloudTokenProperty() {
        ClientConfig config = new ClientConfig();
        config.setProperty("hecaton.client.cloud.discovery.token", "TOKEN");
        assertThatThrownBy(() -> HecatonClient.newHecatonClient(config))
                .isInstanceOf(InvalidConfigurationException.class)
                .hasMessageContaining("Cloud-provider discovery is not supported in Hecaton");
    }
}
