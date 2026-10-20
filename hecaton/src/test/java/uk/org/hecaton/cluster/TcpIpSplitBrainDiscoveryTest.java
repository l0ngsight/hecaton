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

package uk.org.hecaton.cluster;

import uk.org.hecaton.config.AdvancedNetworkConfig;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.NetworkConfig;
import uk.org.hecaton.config.RestApiConfig;
import uk.org.hecaton.config.RestServerEndpointConfig;
import uk.org.hecaton.config.ServerSocketEndpointConfig;
import uk.org.hecaton.config.TcpIpConfig;
import uk.org.hecaton.core.Hecaton;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.ascii.HTTPCommunicator;
import uk.org.hecaton.internal.management.operation.UpdateTcpIpMemberListOperation;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonSerialParametersRunnerFactory;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static uk.org.hecaton.cluster.TcpIpSplitBrainDiscoveryTest.UpdateType.MEMBER_LIST_UPDATE;
import static uk.org.hecaton.cluster.TcpIpSplitBrainDiscoveryTest.UpdateType.WITH_OPERATION;
import static uk.org.hecaton.internal.util.InvocationUtil.invokeOnStableClusterSerial;

@RunWith(HecatonParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HecatonSerialParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class TcpIpSplitBrainDiscoveryTest extends HecatonTestSupport {

    @Parameterized.Parameters(name = "advancedNetwork:{0}, updateType:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {true, MEMBER_LIST_UPDATE},
                {true, WITH_OPERATION},
                {false, MEMBER_LIST_UPDATE},
                {false, WITH_OPERATION}
        });
    }

    @Parameterized.Parameter
    public static boolean advancedNetwork;

    @Parameterized.Parameter(1)
    public static UpdateType updateType;

    List<HecatonInstance> instances = new ArrayList<>();

    @After
    public void cleanup() {
        for (HecatonInstance instance: instances) {
            instance.getLifecycleService().terminate();
        }
    }

    @Test
    public void testSplitBrainRecoveryFromInitialSplit() throws IOException {
        instances.add(Hecaton.newHecatonInstance(createConfigWithRestEnabled(5801, 5901)));
        instances.add(Hecaton.newHecatonInstance(createConfigWithRestEnabled(5901, 5801)));
        assertClusterSizeEventually(2, Arrays.asList(instances.get(0), instances.get(1)));


        instances.add(Hecaton.newHecatonInstance(createConfigWithRestEnabled(6001, 6101)));
        instances.add(Hecaton.newHecatonInstance(createConfigWithRestEnabled(6101, 6001)));
        assertClusterSizeEventually(2, Arrays.asList(instances.get(2), instances.get(3)));
        updateMemberList(3);
        assertClusterSizeEventually(4, instances);
    }

    @Test
    public void testAddressAreStoredForUnreachableNodes() throws IOException {
        instances.add(Hecaton.newHecatonInstance(createConfigWithRestEnabled(5801, 5901)));
        instances.add(Hecaton.newHecatonInstance(createConfigWithRestEnabled(5901, 5801)));
        assertClusterSizeEventually(2, Arrays.asList(instances.get(0), instances.get(1)));
        updateMemberList(1);

        instances.add(Hecaton.newHecatonInstance(createConfigWithRestEnabled(6001, 6101)));
        instances.add(Hecaton.newHecatonInstance(createConfigWithRestEnabled(6101, 6001)));
        // first brain should find second brain eventually
        assertClusterSizeEventually(4, instances);
    }

    protected static Config createConfigWithRestEnabled(int port, int... otherMembersPorts) {
        Config config = new Config()
                .setProperty("hecaton.merge.first.run.delay.seconds", "10")
                .setProperty("hecaton.merge.next.run.delay.seconds", "10");
        JoinConfig joinConfig;
        boolean restEnabled = updateType != WITH_OPERATION;
        if (!advancedNetwork) {
            NetworkConfig networkConfig = config.getNetworkConfig();
            if (restEnabled) {
                RestApiConfig restApiConfig = new RestApiConfig().setEnabled(true).enableAllGroups();
                config.getNetworkConfig().setRestApiConfig(restApiConfig);
            }
            networkConfig.setPortAutoIncrement(false).setPort(port);
            joinConfig = networkConfig.getJoin();
        } else {
            AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
            advancedNetworkConfig.setEnabled(true);
            if (restEnabled) {
                advancedNetworkConfig.setRestEndpointConfig(new RestServerEndpointConfig().enableAllGroups());
            }
            ServerSocketEndpointConfig serverSocketEndpointConfig = new ServerSocketEndpointConfig();
            serverSocketEndpointConfig.setPort(port).setPortAutoIncrement(false);

            advancedNetworkConfig.setMemberEndpointConfig(serverSocketEndpointConfig);
            joinConfig = advancedNetworkConfig.getJoin();
        }
        joinConfig.getAutoDetectionConfig().setEnabled(false);
        joinConfig.getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig().setEnabled(true).clear().addMember("localhost:" + port);
        for (int otherPort : otherMembersPorts) {
            tcpIpConfig.addMember("localhost:" + otherPort);
        }
        return config;
    }

    protected void updateMemberList(int restNodeIndex) throws IOException {
        HTTPCommunicator communicator;
        switch (updateType) {
            case MEMBER_LIST_UPDATE:
                communicator = new HTTPCommunicator(instances.get(restNodeIndex));
                communicator.updateTcpIpMemberList(
                        instances.get(0).getConfig().getClusterName(),
                        "",
                        "localhost:5801, localhost:5901, localhost:6001, localhost:6101");
                break;
            case WITH_OPERATION:
                invokeOnStableClusterSerial(
                        Accessors.getNodeEngineImpl(instances.get(restNodeIndex)),
                        () -> new UpdateTcpIpMemberListOperation(
                                Arrays.asList("localhost:" + 5801, "localhost:" + 5901, "localhost:" + 6001, "localhost:" + 6101)),
                        5
                ).join();
                break;
        }
    }

    protected enum UpdateType {
        CONFIG_UPDATE, MEMBER_LIST_UPDATE, WITH_OPERATION
    }
}
