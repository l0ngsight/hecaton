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
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static uk.org.hecaton.test.HecatonTestSupport.assertContains;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class})
public class ClientOutBoundPortTest {

    @Before
    @After
    public void cleanUp() {
        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
    }

    @Test
    public void clientOutboundPortRangeTest() {
        Config config1 = new Config();
        config1.setClusterName("client-out-test");
        Hecaton.newHecatonInstance(config1);

        ClientConfig config2 = new ClientConfig();
        config2.setClusterName("client-out-test");
        config2.getNetworkConfig().setOutboundPortDefinitions(Arrays.asList("34700", "34703-34705"));
        HecatonInstance client = HecatonClient.newHecatonClient(config2);

        final int port = ((Client) client.getLocalEndpoint()).getSocketAddress().getPort();

        assertContains(Arrays.asList(34700, 34703, 34704, 34705), port);
    }

}
