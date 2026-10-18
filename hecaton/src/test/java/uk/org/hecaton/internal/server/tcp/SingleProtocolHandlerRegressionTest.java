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

package uk.org.hecaton.internal.server.tcp;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.ServerSocketEndpointConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.HecatonInstanceProxy;
import uk.org.hecaton.internal.nio.ascii.TextProtocolClient;
import uk.org.hecaton.internal.server.ServerConnectionManager;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.NightlyTest;
import uk.org.hecaton.test.annotation.Repeat;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetAddress;

import static uk.org.hecaton.instance.EndpointQualifier.CLIENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category(NightlyTest.class)
public class SingleProtocolHandlerRegressionTest extends HecatonTestSupport {

    private static final long ASSERT_TRUE_TIMEOUT_SECS = 20;
    private TextProtocolClient textProtocolClient;

    @After
    public void cleanup() throws IOException {
        if (textProtocolClient != null) {
            textProtocolClient.close();
        }
        Hecaton.shutdownAll();
    }

    /**
     * Regression test for #19756
     */
    @Test
    @Repeat(50)
    public void testWrongProtocolRegressionTest() {
        HecatonInstance instance = Hecaton.newHecatonInstance(createMemberEndpointConfig());
        ServerConnectionManager connectionManager = ((HecatonInstanceProxy) instance).getOriginal().node
                .getServer().getConnectionManager(CLIENT);
        // Get address of the client endpoint
        InetAddress address = instance.getCluster().getLocalMember().getSocketAddress(CLIENT).getAddress();
        int port = instance.getCluster().getLocalMember().getSocketAddress(CLIENT).getPort();
        textProtocolClient = new TextProtocolClient(address, port);
        textProtocolClient.connect();

        assertTrueEventually(() -> assertTrue(textProtocolClient.isConnected()), ASSERT_TRUE_TIMEOUT_SECS);
        assertTrueEventually(() -> assertEquals(1, connectionManager.getConnections().size()), ASSERT_TRUE_TIMEOUT_SECS);
        // Send wrong protocol data to client endpoint
        textProtocolClient.sendData("AAACP2CP2");
        // Assert that connection must be closed on the member side
        assertTrueEventually(
                () -> assertTrue("Connection must be closed on the member side", connectionManager.getConnections().isEmpty()),
                ASSERT_TRUE_TIMEOUT_SECS
        );
    }


    protected Config createMemberEndpointConfig() {
        ServerSocketEndpointConfig endpointConfig = new ServerSocketEndpointConfig();
        endpointConfig.setName("CLIENT")
                .setPort(10000)
                .setPortAutoIncrement(true);

        Config config = new Config();
        config.getAdvancedNetworkConfig()
                .setEnabled(true)
                .setClientEndpointConfig(endpointConfig);
        return config;
    }
}
