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

package uk.org.hecaton.client.map;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.client.util.StaticLB;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.IndexConfig;
import uk.org.hecaton.config.IndexType;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.config.XmlConfigBuilder;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.query.SampleTestObjects;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapWithIndexCreationTest extends HecatonTestSupport {

    private final TestHecatonFactory factory = new TestHecatonFactory();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    /**
     * Given a two member (A, B) cluster, a SINGLE_MEMBER routing client connected to B,
     * attempts to create a map proxy targeting member A.
     */
    @Test
    public void test_createMapWithIndexes_whenProxyCreatedOnMemberOtherThanClientOwner() {
        Config config = new XmlConfigBuilder().build();

        MapConfig mapConfig = config.getMapConfig("test");
        List<IndexConfig> indexConfigs = mapConfig.getIndexConfigs();

        IndexConfig indexConfig = new IndexConfig();
        indexConfig.addAttribute("name");
        indexConfig.setType(IndexType.SORTED);
        indexConfigs.add(indexConfig);

        HecatonInstance hz1 = factory.newHecatonInstance(config);
        HecatonInstance hz2 = factory.newHecatonInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        // ProxyManager#findNextAddressToSendCreateRequest uses the configured load balancer to find the next address
        // to which proxy creation request will be sent. We want this to be member hz1.
        clientConfig.setLoadBalancer(new StaticLB((Member) hz1.getLocalEndpoint()));
        clientConfig.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER);
        // the client only connects to member hz2.
        clientConfig.getNetworkConfig().addAddress(hz2.getCluster().getLocalMember().getAddress().getHost() + ":"
                + hz2.getCluster().getLocalMember().getAddress().getPort());

        HecatonInstance client = factory.newHecatonClient(clientConfig);

        IMap<String, SampleTestObjects.Employee> test = client.getMap("test");
        test.put("foo", new SampleTestObjects.Employee(1, "name", "age", 32, true, 230));
    }
}
