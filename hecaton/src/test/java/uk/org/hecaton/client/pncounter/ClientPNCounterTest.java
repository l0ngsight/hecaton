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

package uk.org.hecaton.client.pncounter;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.ConsistencyLostException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.crdt.pncounter.PNCounter;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;

import static uk.org.hecaton.test.HecatonTestSupport.assertTrueEventually;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientPNCounterTest {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Test(expected = ConsistencyLostException.class)
    public void testClusterRestart() {
        HecatonInstance instance = hecatonFactory.newHecatonInstance();
        HecatonInstance client = hecatonFactory.newHecatonClient();

        PNCounter pnCounter = client.getPNCounter("test");

        pnCounter.incrementAndGet();

        instance.shutdown();
        HecatonInstance newInstance = hecatonFactory.newHecatonInstance();
        UUID newInstanceUuid = newInstance.getLocalEndpoint().getUuid();

        // PN counter invocation targets are determined with the help of the
        // member list. When the client connects to a new cluster, the member list
        // event comes later, and in the meantime, the client operates with
        // the last known member list. At that time, invocation targets are the old
        // member UUIDs for the PN counter. However, when the client cannot find
        // a connection to that old member UUID, it routes the invocation to a
        // random connection, hoping that member can route the invocation
        // to the correct member. That creates a problem, because the old member
        // is not part of the new cluster the client connected to. So, the new
        // member throws TargetNotMemberException saying that the old member
        // is not in its member list. That exception is not retryable for
        // targeted invocations, and that is passed to the user directly, when
        // there are no more targets to retry. That means, we have to wait
        // until the member list updated in the client side to be able to
        // continue working with the PN counter.
        assertTrueEventually(() -> {
            assertTrue(client.getCluster().getMembers()
                    .stream()
                    .anyMatch(m -> m.getUuid().equals(newInstanceUuid))
            );
        });

        pnCounter.incrementAndGet();
    }

}
