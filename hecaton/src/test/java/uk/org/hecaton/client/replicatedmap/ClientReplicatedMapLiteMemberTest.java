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

package uk.org.hecaton.client.replicatedmap;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.ClientNetworkConfig;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.properties.ClientProperty;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.client.util.ConfigRoutingUtil;
import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.replicatedmap.ReplicatedMap;
import uk.org.hecaton.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.HecatonTestSupport.assertClusterSizeEventually;
import static uk.org.hecaton.test.HecatonTestSupport.randomMapName;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HecatonParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientReplicatedMapLiteMemberTest {

    @Parameterized.Parameter
    public RoutingMode routingMode;

    @Parameterized.Parameters(name = "{index}: routingMode={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(RoutingMode.SINGLE_MEMBER, RoutingMode.ALL_MEMBERS);
    }

    private final TestHecatonFactory factory = new TestHecatonFactory();

    private ClientConfig newClientConfig() {
        return ConfigRoutingUtil.newClientConfig(routingMode);
    }

    @After
    public void destroy() {
        factory.terminateAll();
    }

    @Test
    public void testReplicatedMapIsCreated() {
        testReplicatedMapCreated(2, 1, newClientConfig());
    }

    @Test
    public void testReplicatedMapNotCreatedOnOnlyLiteMembers() {
        Assert.assertThrows(ReplicatedMapCantBeCreatedOnLiteMemberException.class, () -> {
            testReplicatedMapCreated(2, 0, newClientConfig());
        });
    }

    @Test
    public void testReplicatedMapNotCreatedOnSingleLiteMember() {
        Assert.assertThrows(ReplicatedMapCantBeCreatedOnLiteMemberException.class, () -> {
            testReplicatedMapCreated(1, 0, newClientConfig());
        });
    }

    private void testReplicatedMapCreated(int numberOfLiteNodes,
                                          int numberOfDataNodes,
                                          ClientConfig clientConfig) {

        createNodes(numberOfLiteNodes, numberOfDataNodes);
        HecatonInstance client = factory.newHecatonClient(clientConfig);
        assertNotNull(client.getReplicatedMap(randomMapName()));
    }

    @Test
    public void testReplicatedMapPut() {
        List<HecatonInstance> instances = createNodes(3, 1);

        ClientConfig clientConfig = newClientConfig();
        if (routingMode == RoutingMode.SINGLE_MEMBER) {
            configureDummyClientConnection(instances.get(0), clientConfig);
        }
        HecatonInstance client = factory.newHecatonClient(clientConfig);
        ReplicatedMap<Object, Object> map = client.getReplicatedMap(randomMapName());
        assertNull(map.put(1, 2));
    }

    private void configureDummyClientConnection(HecatonInstance instance, ClientConfig clientConfig) {
        Address memberAddress = getAddress(instance);
        clientConfig.setProperty(ClientProperty.SHUFFLE_MEMBER_LIST.getName(), "false");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        networkConfig.addAddress(memberAddress.getHost() + ":" + memberAddress.getPort());
    }

    private List<HecatonInstance> createNodes(int numberOfLiteNodes, int numberOfDataNodes) {
        List<HecatonInstance> instances = new ArrayList<>();

        Config liteConfig = new Config().setLiteMember(true);
        for (int i = 0; i < numberOfLiteNodes; i++) {
            instances.add(factory.newHecatonInstance(liteConfig));
        }

        for (int i = 0; i < numberOfDataNodes; i++) {
            instances.add(factory.newHecatonInstance());
        }

        int clusterSize = numberOfLiteNodes + numberOfDataNodes;
        for (HecatonInstance instance : instances) {
            assertClusterSizeEventually(clusterSize, instance);
        }

        return instances;
    }
}
