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

package uk.org.hecaton.client.impl.connection.tcp;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.networking.nio.AbstractAdvancedNetworkIntegrationTest;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.fail;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class AdvancedNetworkingClientCommunicationIntegrationTest extends AbstractAdvancedNetworkIntegrationTest {

    @Test
    public void testClientConnectionToEndpoints() {
        Config config = createCompleteMultiSocketConfig();
        newHecatonInstance(config);

        HecatonInstance client = null;
        try {
            client = createClient(CLIENT_PORT);
            client.getMap("whatever");
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }

        testClientConnectFailOnPort(MEMBER_PORT);
        testClientConnectFailOnPort(WAN1_PORT);
        testClientConnectFailOnPort(REST_PORT);
        testClientConnectFailOnPort(MEMCACHE_PORT);
    }

    private HecatonInstance createClient(int port) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:" + port);
        clientConfig.getNetworkConfig().setConnectionTimeout(3000);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(3000);
        return HecatonClient.newHecatonClient(clientConfig);
    }

    private void testClientConnectFailOnPort(int port) {
        try {
            createClient(port);
            fail("Client connect should throw IllegalStateException for port " + port);
        } catch (IllegalStateException e) {
            // expected
        }
    }
}
