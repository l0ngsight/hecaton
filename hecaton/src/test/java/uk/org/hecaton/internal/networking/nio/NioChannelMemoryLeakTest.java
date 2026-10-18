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
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.server.tcp.TcpServer;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

import static uk.org.hecaton.core.Hecaton.newHecatonInstance;
import static uk.org.hecaton.spi.properties.ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS;
import static uk.org.hecaton.spi.properties.ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS;
import static uk.org.hecaton.spi.properties.ClusterProperty.WAIT_SECONDS_BEFORE_JOIN;
import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getNode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonSerialClassRunner.class)
@Category(SlowTest.class)
public class NioChannelMemoryLeakTest extends HecatonTestSupport {

    @After
    public void cleanUp() {
        Hecaton.shutdownAll();
    }

    @Test
    public void testNioChannelLeak() {
        Config config = getConfig();
        config.setProperty(MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "1");
        config.setProperty(MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "1");

        HecatonInstance instance = Hecaton.newHecatonInstance(config);
        TcpServer networkingService = (TcpServer) getNode(instance).getServer();
        final NioNetworking networking = (NioNetworking) networkingService.getNetworking();

        assertTrueEventually(() -> assertThat(networking.getChannels()).isEmpty());
    }

    @Test
    public void testNioChannelLeak_afterMultipleSplitBrainMerges() {
        Config config = getConfig();
        config.setProperty(WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config.setProperty(MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "99999999");
        config.setProperty(MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "99999999");

        final HecatonInstance instance1 = newHecatonInstance(config);
        final HecatonInstance instance2 = newHecatonInstance(config);
        final HecatonInstance instance3 = newHecatonInstance(config);
        assertClusterSizeEventually(3, instance1, instance2, instance3);

        for (int i = 0; i < 5; i++) {
            sleepSeconds(1);

            closeConnectionBetween(instance1, instance3);
            closeConnectionBetween(instance2, instance3);
            assertClusterSizeEventually(2, instance1, instance2);
            assertClusterSizeEventually(1, instance3);

            getNode(instance3).getClusterService().merge(getAddress(instance1));
            assertClusterSizeEventually(3, instance1, instance2, instance3);
        }

        assertTrueEventually(() -> {
            assertNoChannelLeak(instance1);
            assertNoChannelLeak(instance2);
            assertNoChannelLeak(instance3);
        });
    }

    private void assertNoChannelLeak(HecatonInstance instance) {
        int clusterSize = instance.getCluster().getMembers().size();
        // There may be one or two channels between two members.
        // Ideally there'll be only a single channel,
        // but when two members initiate the connection concurrently,
        // it may end up with two different connections between them.
        int maxChannelCount = (clusterSize - 1) * 2;

        TcpServer networkingService = (TcpServer) getNode(instance).getServer();
        NioNetworking networking = (NioNetworking) networkingService.getNetworking();
        Set<NioChannel> channels = networking.getChannels();

        assertThat(channels.size()).isLessThanOrEqualTo(maxChannelCount);
        for (NioChannel channel : channels) {
            assertTrue("Channel " + channel + " was found closed (channel: " + channel.isClosed() + ", socketChannel: " + !channel
                            .socketChannel().isOpen() + ") in instance " + instance,
                    !channel.isClosed() && channel.socketChannel().isOpen());
        }
    }

    @Override
    protected Config getConfig() {
        Config config = new Config();
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);
        join.getMulticastConfig().setEnabled(false);
        return config;
    }
}
