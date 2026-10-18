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

package uk.org.hecaton.internal.monitor.impl;

import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.internal.monitor.NodeState;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import uk.org.hecaton.version.MemberVersion;
import uk.org.hecaton.version.Version;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NodeStateImplTest {

    @Test
    public void toJson() throws Exception {
        ClusterState clusterState = ClusterState.ACTIVE;
        uk.org.hecaton.instance.impl.NodeState nodeState = uk.org.hecaton.instance.impl.NodeState.PASSIVE;
        Version clusterVersion = Version.of("3.8");
        MemberVersion memberVersion = MemberVersion.of("3.9.0");

        NodeState state = new NodeStateImpl(clusterState, nodeState, clusterVersion, memberVersion);
        NodeState deserialized = new NodeStateImpl();
        deserialized.fromJson(state.toJson());

        assertEquals(clusterState, deserialized.getClusterState());
        assertEquals(nodeState, deserialized.getNodeState());
        assertEquals(clusterVersion, deserialized.getClusterVersion());
        assertEquals(memberVersion, deserialized.getMemberVersion());
    }

}
