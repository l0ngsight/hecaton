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

package uk.org.hecaton.client.flakeidgen.impl;

import uk.org.hecaton.client.config.ClientConfig;
import uk.org.hecaton.client.config.RoutingMode;
import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.flakeidgen.FlakeIdGenerator;
import uk.org.hecaton.cluster.impl.MemberImpl;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FlakeIdGenerator_NodeIdOverflowIntegrationTest {

    private static final ILogger LOGGER = Logger.getLogger(FlakeIdGenerator_NodeIdOverflowIntegrationTest.class);

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final TestHecatonFactory factory = new TestHecatonFactory();
    private HecatonInstance instance2;
    private HecatonInstance instance1;

    @Before
    public void before() {
        Config cfg = new Config();
        cfg.getFlakeIdGeneratorConfig("gen").setPrefetchCount(1);
        instance1 = factory.newHecatonInstance(cfg);
        instance2 = factory.newHecatonInstance(cfg);
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void when_memberOutOfRangeNodeId_then_theOtherMemberUsed() {
        // Design of this test: we create two members. To one of them, out-of-range memberListJoinVersion
        // is assigned. This causes the client message to fail and the client will retry with another member
        // and will never again try the failed member.
        //
        // Probability of initially choosing a good member and not going through the process of choosing another
        // one is 50%. To avoid false success, we retry 10 times, which gives the probability of false success
        // of 0.1%.
        assignOverflowedNodeId(instance2);

        ClientConfig clientConfig = new ClientConfig();
        // use single member routing - such clients must also work reliably
        clientConfig.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.SINGLE_MEMBER);
        for (int i = 0; i < 10; i++) {
            LOGGER.info("Creating client " + i);
            HecatonInstance client = factory.newHecatonClient(clientConfig);
            FlakeIdGenerator gen = client.getFlakeIdGenerator("gen");
            for (int j = 0; j < 100; j++) {
                // call should not fail
                gen.newId();
            }
        }
    }

    @Test
    public void when_allMembersOutOfRangeNodeId_then_error() {
        assignOverflowedNodeId(instance1);
        assignOverflowedNodeId(instance2);

        HecatonInstance client = factory.newHecatonClient();
        FlakeIdGenerator gen = client.getFlakeIdGenerator("gen");

        exception.expect(HecatonException.class);
        exception.expectMessage("All members have node ID out of range");
        gen.newId();
    }

    private static void assignOverflowedNodeId(HecatonInstance instance2) {
        MemberImpl member = (MemberImpl) instance2.getCluster().getLocalMember();
        member.setMemberListJoinVersion(100000);
    }
}
