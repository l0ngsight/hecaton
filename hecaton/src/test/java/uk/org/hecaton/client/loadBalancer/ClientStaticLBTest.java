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

import uk.org.hecaton.client.util.StaticLB;
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
public class ClientStaticLBTest {

    private final TestHecatonInstanceFactory factory = new TestHecatonInstanceFactory();

    @After
    public void after() {
        factory.terminateAll();
    }

    @Test
    public void testStaticLB_withMembers() {
        HecatonInstance server = factory.newHecatonInstance();
        Member member = server.getCluster().getLocalMember();

        StaticLB lb = new StaticLB(member);

        assertEquals(member, lb.next());
        assertEquals(member, lb.nextDataMember());
    }

    @Test
    public void testStaticLB_withLiteMembers() {
        HecatonInstance server = factory.newHecatonInstance(new Config().setLiteMember(true));
        Member member = server.getCluster().getLocalMember();

        StaticLB lb = new StaticLB(member);

        assertEquals(member, lb.next());
        assertNull(lb.nextDataMember());
    }
}
