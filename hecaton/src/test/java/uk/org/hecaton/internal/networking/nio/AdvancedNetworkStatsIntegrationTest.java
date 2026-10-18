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

package uk.org.hecaton.internal.networking.nio;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.instance.ProtocolType;
import uk.org.hecaton.internal.metrics.MetricsRegistry;
import uk.org.hecaton.internal.server.NetworkStats;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.function.Function;

import static uk.org.hecaton.instance.ProtocolType.MEMBER;
import static uk.org.hecaton.test.HecatonTestSupport.assertClusterSizeEventually;
import static uk.org.hecaton.test.HecatonTestSupport.assertNotContains;
import static uk.org.hecaton.test.HecatonTestSupport.assertTrueAllTheTime;
import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.HecatonTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class AdvancedNetworkStatsIntegrationTest extends AbstractAdvancedNetworkIntegrationTest {

    private HecatonInstance instance1;
    private HecatonInstance instance2;

    @Test
    public void testStats_advancedNetworkEnabledAndConnectionActive_readFromEMs() {
        Config config = createCompleteMultiSocketConfig();
        configureTcpIpConfig(config);
        instance1 = newHecatonInstance(config);
        instance2 = startSecondInstance();

        assertTrueEventually(() -> {
            assertTrue(getBytesReceivedFromEMs(instance1, MEMBER) > 0);
            assertTrue(getBytesSentFromEMs(instance1, MEMBER) > 0);
            assertTrue(getBytesReceivedFromEMs(instance2, MEMBER) > 0);
            assertTrue(getBytesSentFromEMs(instance2, MEMBER) > 0);
        });

        assertNonMemberNetworkStatsAreZeroFromEMs(instance1);
        assertNonMemberNetworkStatsAreZeroFromEMs(instance2);
    }

    @Test
    public void testStats_advancedNetworkEnabledAndConnectionActive_readFromMetrics() {
        Config config = createCompleteMultiSocketConfig();
        configureTcpIpConfig(config);
        instance1 = newHecatonInstance(config);
        instance2 = startSecondInstance();

        assertTrueEventually(() -> {
            assertTrue(getBytesReceivedFromMetrics(instance1, MEMBER) > 0);
            assertTrue(getBytesSentFromMetrics(instance1, MEMBER) > 0);
            assertTrue(getBytesReceivedFromMetrics(instance2, MEMBER) > 0);
            assertTrue(getBytesSentFromMetrics(instance2, MEMBER) > 0);
        });

        assertNonMemberNetworkStatsAreZeroFromMetrics(instance1);
        assertNonMemberNetworkStatsAreZeroFromMetrics(instance2);
    }

    @Test
    public void testStats_advancedNetworkEnabledAndConnectionClosed_readFromEMs() {
        Config config = createCompleteMultiSocketConfig();
        configureTcpIpConfig(config);
        instance1 = newHecatonInstance(config);
        instance2 = startSecondInstance();
        assertClusterSizeEventually(2, instance1, instance2);

        instance2.shutdown();
        assertClusterSizeEventually(1, instance1);

        assertTrueEventually(() -> {
            assertTrue(getBytesReceivedFromEMs(instance1, MEMBER) > 0);
            assertTrue(getBytesSentFromEMs(instance1, MEMBER) > 0);
        });

        assertNonMemberNetworkStatsAreZeroFromEMs(instance1);
    }

    @Test
    public void testStats_advancedNetworkEnabledAndConnectionClosed_readFromMetrics() {
        Config config = createCompleteMultiSocketConfig();
        configureTcpIpConfig(config);
        instance1 = newHecatonInstance(config);
        instance2 = startSecondInstance();
        assertClusterSizeEventually(2, instance1, instance2);

        instance2.shutdown();
        assertClusterSizeEventually(1, instance1);

        assertTrueEventually(() -> {
            assertTrue(getBytesReceivedFromMetrics(instance1, MEMBER) > 0);
            assertTrue(getBytesSentFromMetrics(instance1, MEMBER) > 0);
        });

        assertNonMemberNetworkStatsAreZeroFromMetrics(instance1);
    }

    @Test
    public void testStats_advancedNetworkDisabled() {
        instance1 = newHecatonInstance(getUnisocketConfig(MEMBER_PORT));
        instance2 = newHecatonInstance(getUnisocketConfig(MEMBER_PORT + 1));
        assertClusterSizeEventually(2, instance1, instance2);

        assertTrueAllTheTime(() -> {
            assertAllNetworkStatsAreZeroFromEMs(instance1);
            assertAllNetworkStatsAreZeroFromEMs(instance2);

            assertAllNetworkStatsNotRegisteredAsMetrics(instance1);
            assertAllNetworkStatsNotRegisteredAsMetrics(instance2);
        }, 30);
    }

    private Config getUnisocketConfig(int memberPort) {
        Config config1 = smallInstanceConfig();
        config1.getNetworkConfig().setPort(memberPort);
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true)
               .addMember("127.0.0.1:" + MEMBER_PORT)
               .addMember("127.0.0.1:" + (MEMBER_PORT + 1));
        return config1;
    }

    private void assertAllNetworkStatsAreZeroFromEMs(HecatonInstance instance) {
        assertEquals(0, getBytesReceivedFromEMs(instance, MEMBER));
        assertEquals(0, getBytesSentFromEMs(instance, MEMBER));
        assertNonMemberNetworkStatsAreZeroFromEMs(instance);
    }

    private void assertNonMemberNetworkStatsAreZeroFromEMs(HecatonInstance instance) {
        for (ProtocolType protocolType : ProtocolType.values()) {
            if (protocolType != MEMBER) {
                assertEquals(0, getBytesReceivedFromEMs(instance, protocolType));
                assertEquals(0, getBytesSentFromEMs(instance, protocolType));
            }
        }
    }

    private long getBytesReceivedFromEMs(HecatonInstance instance, ProtocolType protocolType) {
        return getBytesTransceivedFromEMs(instance, protocolType, NetworkStats::getBytesReceived);
    }

    private long getBytesSentFromEMs(HecatonInstance instance, ProtocolType protocolType) {
        return getBytesTransceivedFromEMs(instance, protocolType, NetworkStats::getBytesSent);
    }

    private long getBytesTransceivedFromEMs(HecatonInstance instance, ProtocolType protocolType, Function<NetworkStats, Long> getFn) {
        Map<EndpointQualifier, NetworkStats> stats = getNode(instance)
                .getServer()
                .getNetworkStats();
        long bytesTransceived = 0;
        if (stats != null) {
            for (Map.Entry<EndpointQualifier, NetworkStats> entry : stats.entrySet()) {
                if (entry.getKey().getType() == protocolType) {
                    bytesTransceived += getFn.apply(entry.getValue());
                }
            }
        }
        return bytesTransceived;
    }

    private void assertNonMemberNetworkStatsAreZeroFromMetrics(HecatonInstance instance) {
        for (ProtocolType protocolType : ProtocolType.values()) {
            if (protocolType != MEMBER) {
                assertEquals(0, getBytesReceivedFromMetrics(instance, protocolType));
                assertEquals(0, getBytesSentFromMetrics(instance, protocolType));
            }
        }
    }

    private long getBytesReceivedFromMetrics(HecatonInstance instance, ProtocolType protocolType) {
        MetricsRegistry registry = getNode(instance).nodeEngine.getMetricsRegistry();
        return registry.newLongGauge("tcp.bytesReceived." + protocolType.name()).read();
    }

    private long getBytesSentFromMetrics(HecatonInstance instance, ProtocolType protocolType) {
        MetricsRegistry registry = getNode(instance).nodeEngine.getMetricsRegistry();
        return registry.newLongGauge("tcp.bytesSend." + protocolType.name()).read();
    }

    private void assertAllNetworkStatsNotRegisteredAsMetrics(HecatonInstance instance) {
        MetricsRegistry registry = getNode(instance).nodeEngine.getMetricsRegistry();
        for (ProtocolType protocolType : ProtocolType.values()) {
            assertNotContains(registry.getNames(), "tcp.bytesReceived." + protocolType.name());
            assertNotContains(registry.getNames(), "tcp.bytesSend." + protocolType.name());
        }
    }

    private HecatonInstance startSecondInstance() {
        Config config = prepareJoinConfigForSecondMember(MEMBER_PORT);
        HecatonInstance newHzInstance = newHecatonInstance(config);
        int clusterSize = newHzInstance.getCluster().getMembers().size();
        assertEquals(2, clusterSize);
        return newHzInstance;
    }
}
