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

package uk.org.hecaton.client.impl.proxy;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.impl.spi.ClientProxy;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ProxyEqualityTest {

    private final TestHecatonFactory hecatonFactoryClusterA = new TestHecatonFactory();
    private final TestHecatonFactory hecatonFactoryClusterB = new TestHecatonFactory();

    private final String name = "foo";

    private HecatonInstance client1ClusterA;
    private HecatonInstance client2ClusterA;

    private HecatonInstance client1ClusterB;

    @After
    public void tearDown() {
        hecatonFactoryClusterA.terminateAll();
        hecatonFactoryClusterB.terminateAll();
    }


    @Before
    public void setup() throws Exception {
        Config config = new Config();
        String clusterAName = "ClusterA";
        config.setClusterName(clusterAName);
        hecatonFactoryClusterA.newHecatonInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(clusterAName);
        client1ClusterA = hecatonFactoryClusterA.newHecatonClient(clientConfig);
        client2ClusterA = hecatonFactoryClusterA.newHecatonClient(clientConfig);

        //setup Group B
        config = new Config();
        String clusterBName = "ClusterB";
        config.setClusterName(clusterBName);
        hecatonFactoryClusterB.newHecatonInstance(config);

        clientConfig = new ClientConfig();
        clientConfig.setClusterName(clusterBName);
        client1ClusterB = hecatonFactoryClusterB.newHecatonClient(clientConfig);
    }

    @Test
    public void testTwoClientProxiesFromTheSameInstanceAreEquals() {

        ClientProxy ref1 = (ClientProxy) client1ClusterA.getSet(name);
        ClientProxy ref2 = (ClientProxy) client1ClusterA.getSet(name);

        assertEquals(ref1, ref2);
    }

    @Test
    public void testProxiesAreCached() {

        ClientProxy ref1 = (ClientProxy) client1ClusterA.getSet(name);
        ClientProxy ref2 = (ClientProxy) client1ClusterA.getSet(name);

        assertSame(ref1, ref2);
    }

    @Test
    public void testTwoClientProxiesFromDifferentInstancesAreNotEquals() {

        ClientProxy ref1 = (ClientProxy) client1ClusterA.getSet(name);
        ClientProxy ref2 = (ClientProxy) client1ClusterB.getSet(name);

        assertNotEquals(ref1, ref2);
    }

    @Test
    public void testTwoClientProxiesFromTwoDifferentClientsConnectedToTheSameInstanceAreNotEquals() {

        ClientProxy ref1 = (ClientProxy) client1ClusterA.getSet(name);
        ClientProxy ref2 = (ClientProxy) client2ClusterA.getSet(name);

        assertNotEquals(ref1, ref2);
    }
}
