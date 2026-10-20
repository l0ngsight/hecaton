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

package uk.org.hecaton.client.bluegreen;

import uk.org.hecaton.client.HecatonClient;
import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientFailoverConfig;
import uk.org.hecaton.client.impl.ClientSelectors;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.LifecycleEvent.LifecycleState;
import uk.org.hecaton.instance.impl.HecatonInstanceProxy;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Client failover between two clusters (blue/green deployment, F15).
 */
@RunWith(HecatonSerialClassRunner.class)
@Category(QuickTest.class)
public class FailoverTest extends ClientTestSupport {

    private HecatonInstance blue;
    private HecatonInstance green;
    private HecatonInstance client;
    private final CountDownLatch changedCluster = new CountDownLatch(1);

    @Before
    public void setUp() {
        blue = member("blue");
        green = member("green");
        ClientFailoverConfig failoverConfig = new ClientFailoverConfig()
                .addClientConfig(clientConfig("blue", blue))
                .addClientConfig(clientConfig("green", green))
                .setTryCount(10);
        client = HecatonClient.newHecatonFailoverClient(failoverConfig);
        client.getLifecycleService().addLifecycleListener(event -> {
            if (event.getState() == LifecycleState.CLIENT_CHANGED_CLUSTER) {
                changedCluster.countDown();
            }
        });
    }

    @After
    public void cleanUp() {
        HecatonClient.shutdownAll();
        Hecaton.shutdownAll();
    }

    @Test
    public void connectsToTheFirstCluster() {
        client.getMap("m").put("k", "v");

        assertThat(blue.getMap("m").get("k")).isEqualTo("v");
        assertThat(green.getMap("m").size()).isZero();
    }

    @Test
    public void movesToTheNextClusterWhenTheFirstGoesDown() {
        blue.getLifecycleService().terminate();

        assertOpenEventually(changedCluster);
        client.getMap("m").put("k", "v");
        assertThat(green.getMap("m").get("k")).isEqualTo("v");
    }

    @Test
    public void movesToTheNextClusterWhenTheFirstBlocksClients() {
        ((HecatonInstanceProxy) blue).getOriginal().node.getClientEngine().applySelector(ClientSelectors.none());

        assertOpenEventually(changedCluster);
        client.getMap("m").put("k", "v");
        assertThat(green.getMap("m").get("k")).isEqualTo("v");
        assertThat(blue.getMap("m").size()).isZero();
    }

    private static HecatonInstance member(String clusterName) {
        Config config = new Config().setClusterName(clusterName);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        return Hecaton.newHecatonInstance(config);
    }

    private static ClientConfig clientConfig(String clusterName, HecatonInstance member) {
        ClientConfig config = new ClientConfig().setClusterName(clusterName);
        Address address = member.getCluster().getLocalMember().getAddress();
        config.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(5000);
        return config;
    }
}
