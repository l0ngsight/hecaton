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

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientTpcConfig;
import uk.org.hecaton.client.impl.clientside.HecatonClientInstanceImpl;
import uk.org.hecaton.client.impl.connection.ClientConnectionManager;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.test.ClientTestSupport;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static uk.org.hecaton.client.impl.connection.tcp.TcpClientConnectionManager.getTargetTpcPorts;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


@RunWith(HecatonParallelClassRunner.class)
@Category(QuickTest.class)
public class TcpClientConnectionManagerTest extends ClientTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @Before
    public void setup() {
        factory.newHecatonInstance(smallInstanceConfigWithoutJetAndMetrics());
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testGetTargetTpcPorts_whenConnectToAll() {
        ClientTpcConfig config = new ClientTpcConfig();
        List<Integer> tpcPorts = asList(1, 2, 3);

        // when larger than the number of tpc ports, return the full set.
        config.setConnectionCount(tpcPorts.size() + 1);
        assertEquals(tpcPorts, getTargetTpcPorts(tpcPorts, config));

        // when equal than the number of tpc ports, return the full set.
        config.setConnectionCount(tpcPorts.size());
        assertEquals(tpcPorts, getTargetTpcPorts(tpcPorts, config));

        // When 0, return the full set.
        config.setConnectionCount(0);
        assertEquals(tpcPorts, getTargetTpcPorts(tpcPorts, config));
    }

    @Test
    public void testGetTargetTpcPorts_whenConnectToSubset() {
        ClientTpcConfig config = new ClientTpcConfig();

        config.setConnectionCount(2);
        List<Integer> tpcPorts = asList(1, 2, 3);
        List<Integer> result = getTargetTpcPorts(tpcPorts, config);

        assertEquals(2, result.size());
        assertTrue(tpcPorts.containsAll(result));
    }

    @Test
    public void testIsSingleMemberClient_whenTpcDisabledAndAllMembersRoutingDisabled() {
        verifyIsSingleMemberClient(false, false);
    }

    @Test
    @Ignore("TPC only supports ALL_MEMBERS routing after the introduction of MULTI_MEMBER routing")
    public void testIsSingleMemberClient_whenTpcEnabledAndAllMembersRoutingDisabled() {
        verifyIsSingleMemberClient(true, false);
    }

    @Test
    public void testIsSingleMemberClient_whenTpcDisabledAndAllMembersRoutingEnabled() {
        verifyIsSingleMemberClient(false, true);
    }

    @Test
    public void testIsSingleMemberClient_whenTpcEnabledAndAllMembersRoutingEnabled() {
        verifyIsSingleMemberClient(true, true);
    }

    private void verifyIsSingleMemberClient(boolean tpcEnabled, boolean allMembersRouting) {
        ClientConfig config = new ClientConfig();
        config.getTpcConfig().setEnabled(tpcEnabled);
        config.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(allMembersRouting
                ? RoutingMode.ALL_MEMBERS : RoutingMode.SINGLE_MEMBER);

        HecatonInstance client = factory.newHecatonClient(config);
        HecatonClientInstanceImpl clientImpl = getHecatonClientInstanceImpl(client);

        ClientConnectionManager connectionManager = clientImpl.getConnectionManager();
        boolean isSingleMember = connectionManager.getRoutingMode() == RoutingMode.SINGLE_MEMBER;
        // should be SINGLE_MEMBER routing only when ALL_MEMBERS routing is not set and TPC disabled
        assertEquals(!allMembersRouting && !tpcEnabled, isSingleMember);
    }

    @Test
    public void testSkipMemberListDuringReconnection() {
        HecatonInstance instance = factory.newHecatonInstance(smallInstanceConfigWithoutJetAndMetrics());

        Address address = instance.getCluster().getLocalMember().getAddress();
        String addressString = address.getHost() + ":" + address.getPort();
        ClientConfig config = new ClientConfig();
        config.setProperty("hecaton.client.internal.skip.member.list.during.reconnection", "true");
        config.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER);

        config.getNetworkConfig().addAddress(addressString);
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(3_000);

        // There are two members, and the SINGLE_MEMBER routing client is connecting
        // to one of them. (the address of the `instance` defined above)
        HecatonInstance client = factory.newHecatonClient(config);

        assertEquals(2, client.getCluster().getMembers().size());
        instance.shutdown();

        // We shut down the `instance` the client is connected to but
        // there is still a member running. If the client was to try to
        // connect to members from the member list, it would succeed
        // and the assertion below would never be true.
        assertTrueEventually(() -> assertFalse(client.getLifecycleService().isRunning()));
    }
}
