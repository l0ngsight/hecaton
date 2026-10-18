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

package uk.org.hecaton.client;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.cluster.Member;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.JoinConfig;
import uk.org.hecaton.config.MemberAttributeConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMemberAttributeTest extends HecatonTestSupport {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void cleanup() {
        hecatonFactory.terminateAll();
    }

    @Test(timeout = 120000)
    public void testConfigAttributes() {
        Config c = new Config();
        JoinConfig join = c.getNetworkConfig().getJoin();
        join.getTcpIpConfig().addMember("127.0.0.1").setEnabled(true);
        join.getMulticastConfig().setEnabled(false);
        MemberAttributeConfig memberAttributeConfig = c.getMemberAttributeConfig();
        memberAttributeConfig.setAttribute("Test", "123");

        HecatonInstance h1 = hecatonFactory.newHecatonInstance(c);
        Member m1 = h1.getCluster().getLocalMember();
        assertEquals("123", m1.getAttribute("Test"));

        HecatonInstance h2 = hecatonFactory.newHecatonInstance(c);
        Member m2 = h2.getCluster().getLocalMember();
        assertEquals("123", m2.getAttribute("Test"));

        assertClusterSize(2, h2);

        Member member = null;
        for (Member m : h2.getCluster().getMembers()) {
            if (m.equals(h2.getCluster().getLocalMember())) {
                continue;
            }
            member = m;
        }

        assertNotNull(member);
        assertEquals(m1, member);
        assertNotNull(member.getAttribute("Test"));
        assertEquals("123", member.getAttribute("Test"));

        HecatonInstance client = hecatonFactory.newHecatonClient();
        Collection<Member> members = client.getCluster().getMembers();
        for (Member m : members) {
            assertEquals("123", m.getAttribute("Test"));
        }
    }
}
