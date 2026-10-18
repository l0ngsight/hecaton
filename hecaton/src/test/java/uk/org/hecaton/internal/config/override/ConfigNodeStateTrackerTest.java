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

package uk.org.hecaton.internal.config.override;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.impl.YamlClientDomConfigProcessor;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.internal.config.YamlMemberDomConfigProcessor;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category(QuickTest.class)
public class ConfigNodeStateTrackerTest {

    @Test
    public void shouldDetectUnappliedMemberConfigEntries() throws Exception {
        Map<String, String> entries = new HashMap<>();
        entries.put("HC_CLUSTERNAME", "foo");
        entries.put("HC_CLUSTRNAME", "foo");
        entries.put("HC_NETWORK_PORT", "5702");
        entries.put("HC_NETWORK_JOIN_TCPIP_BLE", "false");
        entries.put("HC_NETWORK_JOIN_MULTCAST_ENABLED", "false");

        ConfigNode configNode = PropertiesToNodeConverter.propsToNode(EnvVariablesConfigParser.member().parse(entries));

        new YamlMemberDomConfigProcessor(true, new Config(), false)
          .buildConfig(new ConfigOverrideElementAdapter(configNode));

        Map<String, String> unprocessed = new ConfigNodeStateTracker().unprocessedNodes(configNode);
        assertTrue(unprocessed.containsKey("hecaton.network.port"));
        assertTrue(unprocessed.containsKey("hecaton.clustrname"));
        assertTrue(unprocessed.containsKey("hecaton.network.join.tcpip.ble"));
        assertTrue(unprocessed.containsKey("hecaton.network.join.multcast.enabled"));
    }

    @Test
    public void shouldDetectUnappliedClientConfigEntries() {
        Map<String, String> entries = new HashMap<>();
        entries.put("HCCLIENT_FOO", "foo");
        entries.put("HCCLIENT_NETWORK_SOCKETINTERCEPTOR_ENABLE", "true");
        entries.put("HCCLIENT_NETWORK_SMARTROUTING", "true");

        ConfigNode configNode = PropertiesToNodeConverter.propsToNode(EnvVariablesConfigParser.client().parse(entries));

        new YamlClientDomConfigProcessor(true, new ClientConfig(), false)
          .buildConfig(new ConfigOverrideElementAdapter(configNode));

        Map<String, String> unprocessed = new ConfigNodeStateTracker().unprocessedNodes(configNode);
        assertTrue(unprocessed.containsKey("hecaton-client.foo"));
        assertTrue(unprocessed.containsKey("hecaton-client.network.socketinterceptor.enable"));
        assertFalse(unprocessed.containsKey("hecaton-client.network.smartrouting"));
    }
}
