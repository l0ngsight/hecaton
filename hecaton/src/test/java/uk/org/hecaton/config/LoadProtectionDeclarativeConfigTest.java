/*
 * Copyright (c) 2026, The Hecaton Authors.
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

package uk.org.hecaton.config;

import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.org.hecaton.config.ConfigTest.HECATON_END_TAG;
import static uk.org.hecaton.config.ConfigTest.HECATON_START_TAG;

/** Load protection (F23) in XML and YAML configuration, and in generated configuration. */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LoadProtectionDeclarativeConfigTest {

    private static final String XML = HECATON_START_TAG
            + "<map name=\"prices\"><immutable>true</immutable></map>\n"
            + "<queue name=\"jobs\"><shard-count>8</shard-count></queue>\n"
            + "<queue name=\"legacy\"><shard-names><shard-name>legacy-0</shard-name>"
            + "<shard-name>legacy-1</shard-name></shard-names></queue>\n"
            + "<load-protection enabled=\"true\">\n"
            + "  <immutable-map-near-cache-size>500</immutable-map-near-cache-size>\n"
            + "  <outbound>\n"
            + "    <max-pending-bytes-per-connection>1000000</max-pending-bytes-per-connection>\n"
            + "    <max-physical-pending-bytes>5000000000</max-physical-pending-bytes>\n"
            + "    <slow-consumer-timeout-seconds>10</slow-consumer-timeout-seconds>\n"
            + "  </outbound>\n"
            + "  <admission>\n"
            + "    <max-in-flight-requests-per-connection>64</max-in-flight-requests-per-connection>\n"
            + "    <max-in-flight-requests>50000</max-in-flight-requests>\n"
            + "    <max-message-bytes>2000000</max-message-bytes>\n"
            + "    <heap-reject-threshold>0.8</heap-reject-threshold>\n"
            + "  </admission>\n"
            + "  <maintenance>\n"
            + "    <thread-count>3</thread-count>\n"
            + "    <max-backlog>200</max-backlog>\n"
            + "    <max-accepts-per-second>100</max-accepts-per-second>\n"
            + "    <max-concurrent-authentications>16</max-concurrent-authentications>\n"
            + "  </maintenance>\n"
            + "  <connection-buffers>\n"
            + "    <pooled>false</pooled>\n"
            + "    <client-buffer-bytes>65536</client-buffer-bytes>\n"
            + "    <max-pooled-bytes>3000000000</max-pooled-bytes>\n"
            + "  </connection-buffers>\n"
            + "</load-protection>\n"
            + HECATON_END_TAG;

    private static final String YAML = ""
            + "hecaton:\n"
            + "  map:\n"
            + "    prices:\n"
            + "      immutable: true\n"
            + "  queue:\n"
            + "    jobs:\n"
            + "      shard-count: 8\n"
            + "    legacy:\n"
            + "      shard-names: [legacy-0, legacy-1]\n"
            + "  load-protection:\n"
            + "    enabled: true\n"
            + "    immutable-map-near-cache-size: 500\n"
            + "    outbound:\n"
            + "      max-pending-bytes-per-connection: 1000000\n"
            + "      max-physical-pending-bytes: 5000000000\n"
            + "      slow-consumer-timeout-seconds: 10\n"
            + "    admission:\n"
            + "      max-in-flight-requests-per-connection: 64\n"
            + "      max-in-flight-requests: 50000\n"
            + "      max-message-bytes: 2000000\n"
            + "      heap-reject-threshold: 0.8\n"
            + "    maintenance:\n"
            + "      thread-count: 3\n"
            + "      max-backlog: 200\n"
            + "      max-accepts-per-second: 100\n"
            + "      max-concurrent-authentications: 16\n"
            + "    connection-buffers:\n"
            + "      pooled: false\n"
            + "      client-buffer-bytes: 65536\n"
            + "      max-pooled-bytes: 3000000000\n";

    @Test
    public void xml() {
        assertConfigured(new InMemoryXmlConfig(XML));
    }

    @Test
    public void yaml() {
        assertConfigured(new InMemoryYamlConfig(YAML));
    }

    @Test
    public void generatedXml() {
        Config config = new InMemoryXmlConfig(XML);
        assertConfigured(new InMemoryXmlConfig(new ConfigXmlGenerator(true, false).generate(config)));
    }

    @Test
    public void offByDefault() {
        Config config = new InMemoryXmlConfig(HECATON_START_TAG + HECATON_END_TAG);
        assertThat(config.getLoadProtectionConfig()).isEqualTo(new LoadProtectionConfig());
        assertThat(config.getLoadProtectionConfig().isEnabled()).isFalse();
    }

    private static void assertConfigured(Config config) {
        assertThat(config.getMapConfig("prices").isImmutable()).isTrue();
        assertThat(config.getQueueConfig("jobs").getShardCount()).isEqualTo(8);
        assertThat(config.getQueueConfig("legacy").getShardNames()).isEqualTo(List.of("legacy-0", "legacy-1"));

        LoadProtectionConfig expected = new LoadProtectionConfig().setEnabled(true).setImmutableMapNearCacheSize(500);
        expected.getOutboundConfig().setMaxPendingBytesPerConnection(1_000_000).setMaxPhysicalPendingBytes(5_000_000_000L)
                .setSlowConsumerTimeoutSeconds(10);
        expected.getAdmissionConfig().setMaxInFlightRequestsPerConnection(64).setMaxInFlightRequests(50_000)
                .setMaxMessageBytes(2_000_000).setHeapRejectThreshold(0.8f);
        expected.getMaintenanceConfig().setThreadCount(3).setMaxBacklog(200).setMaxAcceptsPerSecond(100)
                .setMaxConcurrentAuthentications(16);
        expected.getConnectionBufferConfig().setPooled(false).setClientBufferBytes(65_536).setMaxPooledBytes(3_000_000_000L);
        assertThat(config.getLoadProtectionConfig()).isEqualTo(expected);
    }
}
