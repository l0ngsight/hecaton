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

package uk.org.hecaton.flakeidgen.impl;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonException;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.flakeidgen.FlakeIdGenerator;
import uk.org.hecaton.cluster.impl.MemberImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FlakeIdGenerator_NodeIdOverflowIntegrationTest {

    private final TestHecatonInstanceFactory factory = new TestHecatonInstanceFactory();
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
        assignOutOfRangeNodeId(instance2);

        // let's use the instance with out-of-range node ID to generate IDs, it should succeed
        FlakeIdGenerator gen = instance2.getFlakeIdGenerator("gen");
        for (int i = 0; i < 100; i++) {
            gen.newId();
        }
    }

    @Test
    public void when_allMembersOutOfRangeNodeId_then_error() {
        assignOutOfRangeNodeId(instance1);
        assignOutOfRangeNodeId(instance2);

        FlakeIdGenerator gen = instance1.getFlakeIdGenerator("gen");

        assertThatThrownBy(gen::newId)
                .isInstanceOf(HecatonException.class)
                .hasMessageContaining("All members have node ID out of range");
    }

    private static void assignOutOfRangeNodeId(HecatonInstance instance) {
        MemberImpl member = (MemberImpl) instance.getCluster().getLocalMember();
        member.setMemberListJoinVersion(100000);
    }
}
