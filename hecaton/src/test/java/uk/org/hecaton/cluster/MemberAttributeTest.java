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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MemberAttributeConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MemberAttributeTest extends HecatonTestSupport {

    @Test(timeout = 120000)
    public void testConfigAttributes() {
        Config config = new Config();
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(2);

        MemberAttributeConfig memberAttributeConfig = config.getMemberAttributeConfig();
        memberAttributeConfig.setAttribute("Test", "123");

        HecatonInstance h1 = factory.newHecatonInstance(config);
        Member m1 = h1.getCluster().getLocalMember();
        assertEquals("123", m1.getAttribute("Test"));

        HecatonInstance h2 = factory.newHecatonInstance(config);
        Member m2 = h2.getCluster().getLocalMember();
        assertEquals("123", m2.getAttribute("Test"));

        assertClusterSize(2, h1, h2);

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

        for (Member m : h1.getCluster().getMembers()) {
            if (m == h1.getCluster().getLocalMember()) {
                continue;
            }
            member = m;
        }

        assertNotNull(member);
        assertEquals(m2, member);
        assertNotNull(member.getAttribute("Test"));
        assertEquals("123", member.getAttribute("Test"));

        h1.shutdown();
        h2.shutdown();
    }



    @Test(timeout = 120000)
    public void testCommandLineAttributes() {
        System.setProperty("hecaton.member.attribute.Test-2", "1234");
        System.setProperty("hecaton.member.attribute.Test-3", "12345");
        System.setProperty("hecaton.member.attribute.Test-4", "123456");

        Config config = new Config();
        config.getMemberAttributeConfig().setAttribute("Test-1", "123");
        config.getMemberAttributeConfig().setAttribute("Test-2", "123");

        HecatonInstance h1 = createHecatonInstance(config);
        Member m1 = h1.getCluster().getLocalMember();

        assertEquals("123", m1.getAttribute("Test-1"));
        assertEquals("1234", m1.getAttribute("Test-2"));
        assertEquals("12345", m1.getAttribute("Test-3"));

        h1.shutdown();
    }
}
