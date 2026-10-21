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

package uk.org.hecaton.wan.impl.batch;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.ServerSocketEndpointConfig;
import uk.org.hecaton.config.WanBatchPublisherConfig;
import uk.org.hecaton.config.EndpointConfig;
import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.config.WanReplicationRef;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spi.merge.PassThroughMergePolicy;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * WAN replication between clusters connected over TCP: WAN endpoints of the advanced network, and
 * a publisher moving to the next target endpoint when one is down.
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class WanNetworkReplicationTest extends HecatonTestSupport {

    private static final String WAN = "to-b";
    private static final String MAP = "replicated";
    private static final int ENTRIES = 500;

    private final List<HecatonInstance> instances = new ArrayList<>();

    @After
    public void tearDown() {
        instances.forEach(instance -> instance.getLifecycleService().terminate());
    }

    @Test
    public void replicatesThroughWanEndpointsOfTheAdvancedNetwork() {
        int targetMemberPort = freePort();
        int targetWanPort = freePort();
        Config targetConfig = config("b");
        targetConfig.getAdvancedNetworkConfig().setEnabled(true)
                .setMemberEndpointConfig(new ServerSocketEndpointConfig().setPort(targetMemberPort))
                .addWanEndpointConfig(new ServerSocketEndpointConfig().setName("wan").setPort(targetWanPort));
        join(targetConfig.getAdvancedNetworkConfig().getJoin(), targetMemberPort);
        HecatonInstance target = start(targetConfig);

        int sourcePort = freePort();
        Config sourceConfig = config("a");
        sourceConfig.getAdvancedNetworkConfig().setEnabled(true)
                .setMemberEndpointConfig(new ServerSocketEndpointConfig().setPort(sourcePort))
                .addWanEndpointConfig(new EndpointConfig().setName("out"));
        join(sourceConfig.getAdvancedNetworkConfig().getJoin(), sourcePort);
        replicateTo(sourceConfig, "127.0.0.1:" + targetWanPort).setEndpoint("out");
        HecatonInstance source = start(sourceConfig);

        IMap<Integer, String> map = source.getMap(MAP);
        for (int i = 0; i < ENTRIES; i++) {
            map.put(i, "value" + i);
        }
        assertTrueEventually(() -> assertThat(target.getMap(MAP).size()).isEqualTo(ENTRIES));
    }

    @Test
    public void publisherMovesToTheNextEndpointWhenOneIsDown() {
        int deadPort = freePort();
        int[] targetPorts = {freePort(), freePort()};
        List<HecatonInstance> target = new ArrayList<>();
        for (int port : targetPorts) {
            Config config = config("b");
            config.getNetworkConfig().setPort(port).setPortAutoIncrement(false);
            join(config.getNetworkConfig().getJoin(), targetPorts);
            target.add(start(config));
        }
        assertClusterSizeEventually(2, target);

        int sourcePort = freePort();
        Config sourceConfig = config("a");
        sourceConfig.getNetworkConfig().setPort(sourcePort).setPortAutoIncrement(false);
        join(sourceConfig.getNetworkConfig().getJoin(), sourcePort);
        replicateTo(sourceConfig, "127.0.0.1:" + deadPort + ",127.0.0.1:" + targetPorts[0] + ",127.0.0.1:" + targetPorts[1]);
        HecatonInstance source = start(sourceConfig);

        IMap<Integer, String> map = source.getMap(MAP);
        for (int i = 0; i < ENTRIES; i++) {
            map.put(i, "value" + i);
        }
        assertTrueEventually(() -> assertThat(target.get(1).getMap(MAP).size()).isEqualTo(ENTRIES));

        target.get(0).getLifecycleService().terminate();
        for (int i = ENTRIES; i < 2 * ENTRIES; i++) {
            map.put(i, "value" + i);
        }
        assertTrueEventually(() -> assertThat(target.get(1).getMap(MAP).size()).isEqualTo(2 * ENTRIES));
    }

    private static Config config(String clusterName) {
        Config config = smallInstanceConfig().setClusterName(clusterName);
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "31");
        config.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        return config;
    }

    private static WanBatchPublisherConfig replicateTo(Config config, String endpoints) {
        WanBatchPublisherConfig publisher = new WanBatchPublisherConfig().setClusterName("b")
                .setTargetEndpoints(endpoints).setBatchSize(100).setBatchMaxDelayMillis(50)
                .setResponseTimeoutMillis(5000);
        config.addWanReplicationConfig(new WanReplicationConfig().setName(WAN).addBatchReplicationPublisherConfig(publisher));
        config.addMapConfig(new MapConfig(MAP).setWanReplicationRef(new WanReplicationRef().setName(WAN)
                .setMergePolicyClassName(PassThroughMergePolicy.class.getName())));
        return publisher;
    }

    private static void join(JoinConfig join, int... ports) {
        join.getMulticastConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        for (int port : ports) {
            join.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1:" + port);
        }
    }

    private HecatonInstance start(Config config) {
        HecatonInstance instance = Hecaton.newHecatonInstance(config);
        instances.add(instance);
        return instance;
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
