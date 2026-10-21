/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.cp.internal;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.exception.NotLeaderException;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static uk.org.hecaton.cp.internal.raft.QueryPolicy.ANY_LOCAL;
import static uk.org.hecaton.cp.internal.raft.QueryPolicy.LEADER_LOCAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RaftInvocationManagerQueryTest extends HecatonRaftTestSupport {

    private HecatonInstance[] instances;

    @Test
    public void when_queryLocalFromLeader_withoutAnyCommit_thenReturnDefaultValue() throws Exception {
        int nodeCount = 3;
        instances = newInstances(nodeCount);

        RaftInvocationManager invocationService = getRaftInvocationManager(instances[0]);
        CPGroupId groupId = invocationService.createRaftGroup("test", nodeCount).get();

        Future<Object> future = invocationService.query(groupId, new RaftTestQueryOp(), LEADER_LOCAL);
        assertNull(future.get());
    }

    @Test
    public void when_queryLocalFromFollower_withoutAnyCommit_thenReturnDefaultValue() throws Exception {
        int nodeCount = 3;
        instances = newInstances(nodeCount);

        RaftInvocationManager invocationService = getRaftInvocationManager(instances[0]);
        CPGroupId groupId = invocationService.createRaftGroup("test", nodeCount).get();

        HecatonInstance follower = getRandomFollowerInstance(instances, groupId);

        Future<Object> future = getRaftInvocationManager(follower).query(groupId, new RaftTestQueryOp(), ANY_LOCAL);
        assertNull(future.get());
    }

    @Test
    public void when_queryLocalFromLeader_onStableCluster_thenReadLatestValue() throws Exception {
        int nodeCount = 3;
        instances = newInstances(nodeCount);

        RaftInvocationManager invocationService = getRaftInvocationManager(instances[0]);
        CPGroupId groupId = invocationService.createRaftGroup("test", nodeCount).get();

        String value = "value";
        invocationService.invoke(groupId, new RaftTestApplyOp(value)).get();

        Future<Object> future = invocationService.query(groupId, new RaftTestQueryOp(), LEADER_LOCAL);

        assertEquals(value, future.get());
    }

    @Test
    public void when_queryLocalFromFollower_onStableCluster_thenReadLatestValueEventually() throws Exception {
        int nodeCount = 3;
        instances = newInstances(nodeCount);

        RaftInvocationManager invocationService = getRaftInvocationManager(instances[0]);
        CPGroupId groupId = invocationService.createRaftGroup("test", nodeCount).get();

        String value = "value";
        invocationService.invoke(groupId, new RaftTestApplyOp(value)).get();

        HecatonInstance follower = getRandomFollowerInstance(instances, groupId);
        assertTrueEventually(() -> {
            Future<Object> future = getRaftInvocationManager(follower).query(groupId, new RaftTestQueryOp(), ANY_LOCAL);
            assertEquals(value, future.get());
        });
    }

    @Test
    public void when_queryLocalFromFollower_withLeaderLocalPolicy_thenFail() throws Exception {
        int nodeCount = 3;
        instances = newInstances(nodeCount);

        RaftInvocationManager invocationService = getRaftInvocationManager(instances[0]);
        CPGroupId groupId = invocationService.createRaftGroup("test", nodeCount).get();

        String value = "value";
        invocationService.invoke(groupId, new RaftTestApplyOp(value)).get();

        HecatonInstance follower = getRandomFollowerInstance(instances, groupId);
        Future<Object> future = getRaftInvocationManager(follower).queryLocally(groupId, new RaftTestQueryOp(), LEADER_LOCAL);
        try {
            future.get();
        } catch (ExecutionException e) {
            assertInstanceOf(NotLeaderException.class, e.getCause());
        }
    }

    @Test
    public void when_queryLocalFromLeader_withLeaderLocalPolicy_thenReadLatestValue() throws Exception {
        int nodeCount = 3;
        instances = newInstances(nodeCount);

        RaftInvocationManager invocationService = getRaftInvocationManager(instances[0]);
        CPGroupId groupId = invocationService.createRaftGroup("test", nodeCount).get();

        String value = "value";
        invocationService.invoke(groupId, new RaftTestApplyOp(value)).get();

        HecatonInstance leader = getLeaderInstance(instances, groupId);

        Future<Object> future = getRaftInvocationManager(leader).queryLocally(groupId, new RaftTestQueryOp(), LEADER_LOCAL);
        assertEquals(value, future.get());
    }
}
