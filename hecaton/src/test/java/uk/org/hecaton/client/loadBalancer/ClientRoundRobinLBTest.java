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

package uk.org.hecaton.client.loadBalancer;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.util.RoundRobinLB;
import uk.org.hecaton.cluster.Cluster;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientRoundRobinLBTest {

    private final TestHecatonInstanceFactory factory = new TestHecatonInstanceFactory();

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testRoundRobinLB_withoutMembers() {
        RoundRobinLB lb = new RoundRobinLB();

        assertNull(lb.next());
        assertNull(lb.nextDataMember());
    }

    @Test
    public void testRoundRobinLB_withMembers() {
        RoundRobinLB roundRobinLB = new RoundRobinLB();
        HecatonInstance server = factory.newHecatonInstance();

        Cluster cluster = server.getCluster();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLoadBalancer(roundRobinLB);

        roundRobinLB.init(cluster, clientConfig);

        Member member = cluster.getLocalMember();

        assertEquals(member, roundRobinLB.next());
        assertEquals(member, roundRobinLB.nextDataMember());
    }

    @Test
    public void testRoundRobinLB_withLiteMembers() {
        RoundRobinLB roundRobinLB = new RoundRobinLB();
        HecatonInstance server = factory.newHecatonInstance(new Config().setLiteMember(true));

        Cluster cluster = server.getCluster();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLoadBalancer(roundRobinLB);

        roundRobinLB.init(cluster, clientConfig);

        Member member = cluster.getLocalMember();

        assertEquals(member, roundRobinLB.next());
        assertNull(roundRobinLB.nextDataMember());
    }
}
