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

package uk.org.hecaton.internal.util.phonehome;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.test.TestAwareClientFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static uk.org.hecaton.internal.util.phonehome.PhoneHomeMetrics.ALL_MEMBERS_CLIENTS_COUNT;
import static uk.org.hecaton.internal.util.phonehome.PhoneHomeMetrics.SINGLE_MEMBER_CLIENTS_COUNT;
import static uk.org.hecaton.test.Accessors.getNode;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientRoutingModePhoneHomeTest extends HecatonTestSupport {

    private final TestAwareClientFactory factory = new TestAwareClientFactory();
    private HecatonInstance instance;
    private PhoneHome phoneHome;
    private Map<String, String> parameters;


    @Before
    public void initialise() {
        instance = factory.newHecatonInstance(new Config());
        Node node = getNode(instance);
        phoneHome = new PhoneHome(node);
    }

    @After
    public void after() {
        factory.terminateAll();
    }

    private void refreshMetrics() {
        parameters = phoneHome.phoneHome(true);
    }

    private void newPhoneHome(HecatonInstance instance) {
        phoneHome = new PhoneHome(getNode(instance));
        refreshMetrics();
    }

    @Test
    public void testMultipleClientsRegistered() {
        ClientConfig allMembersClient = new ClientConfig();
        allMembersClient.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.ALL_MEMBERS);
        ClientConfig singleMemberClient = new ClientConfig();
        allMembersClient.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER);

        newPhoneHome(instance);
        assertThat(get(ALL_MEMBERS_CLIENTS_COUNT)).isEqualTo("0");
        assertThat(get(ALL_MEMBERS_CLIENTS_COUNT)).isEqualTo("0");

        factory.newHecatonClient(allMembersClient);
        factory.newHecatonClient(singleMemberClient);

        newPhoneHome(instance);

        assertThat(get(SINGLE_MEMBER_CLIENTS_COUNT)).isEqualTo("1");
        assertThat(get(ALL_MEMBERS_CLIENTS_COUNT)).isEqualTo("1");

        factory.newHecatonClient(allMembersClient);
        factory.newHecatonClient(allMembersClient);
        factory.newHecatonClient(singleMemberClient);
        factory.newHecatonClient(singleMemberClient);

        newPhoneHome(instance);
        assertThat(get(SINGLE_MEMBER_CLIENTS_COUNT)).isEqualTo("3");
        assertThat(get(ALL_MEMBERS_CLIENTS_COUNT)).isEqualTo("3");
    }

    @Test
    public void testMultipleClientsRemoved() {
        ClientConfig allMembersClient = new ClientConfig();
        allMembersClient.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.ALL_MEMBERS);
        ClientConfig singleMemberClient = new ClientConfig();
        singleMemberClient.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER);

        HecatonInstance client1 = factory.newHecatonClient(allMembersClient);
        HecatonInstance client2 = factory.newHecatonClient(singleMemberClient);

        newPhoneHome(instance);
        assertThat(get(SINGLE_MEMBER_CLIENTS_COUNT)).isEqualTo("1");
        assertThat(get(ALL_MEMBERS_CLIENTS_COUNT)).isEqualTo("1");

        // terminate ALL_MEMBERS routing client
        client1.shutdown();

        assertTrueEventually(() -> {
            newPhoneHome(instance);
            assertThat(get(SINGLE_MEMBER_CLIENTS_COUNT)).isEqualTo("1");
            assertThat(get(ALL_MEMBERS_CLIENTS_COUNT)).isEqualTo("0");
        });

        // terminate SINGLE_MEMBER routing client
        client2.shutdown();

        assertTrueEventually(() -> {
            newPhoneHome(instance);
            assertThat(get(SINGLE_MEMBER_CLIENTS_COUNT)).isEqualTo("0");
            assertThat(get(ALL_MEMBERS_CLIENTS_COUNT)).isEqualTo("0");
        });
    }

    private String get(Metric metric) {
        return parameters.get(metric.getQueryParameter());
    }
}
