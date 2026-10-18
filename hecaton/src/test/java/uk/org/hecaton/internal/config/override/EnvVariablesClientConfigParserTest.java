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

import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category(QuickTest.class)
public class EnvVariablesClientConfigParserTest extends HecatonTestSupport {

    private final EnvVariablesConfigParser envVariablesConfigParser = EnvVariablesConfigParser.client();

    @Test
    public void shouldParseEntries() {
        Map<String, String> entries = new HashMap<>();
        entries.put("HCCLIENT_CLUSTERNAME", "foo");
        entries.put("HCCLIENT_NETWORK_JOIN_TCPIP_ENABLED", "false");
        entries.put("HCCLIENT_NETWORK_JOIN_MULTICAST_ENABLED", "false");

        Map<String, String> result = envVariablesConfigParser.parse(entries);

        assertContains(result.entrySet(), entry("hecaton-client.clustername", "foo"));
        assertContains(result.entrySet(), entry("hecaton-client.network.join.tcpip.enabled", "false"));
        assertContains(result.entrySet(), entry("hecaton-client.network.join.multicast.enabled", "false"));
    }

    @Test
    public void shouldIgnoreEntriesWithWrongPrefixes() {
        Map<String, String> entries = new HashMap<>();
        entries.put("HC_CLUSTER__NAME", "foo");
        entries.put("HC_CLIENTNETWORK_JOIN_TCP__IP_ENABLED", "false");
        entries.put("NETWORK_JOIN_MULTICAST_ENABLED", "false");

        Map<String, String> result = envVariablesConfigParser.parse(entries);

        assertEquals(0, result.size());
    }

    private static <K, V> Map.Entry<K, V> entry(K key, V value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }
}
