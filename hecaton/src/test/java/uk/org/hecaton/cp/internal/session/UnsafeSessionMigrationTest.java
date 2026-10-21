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

package uk.org.hecaton.cp.internal.session;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.internal.HecatonRaftTestSupport;
import uk.org.hecaton.cp.internal.RaftGroupId;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnsafeSessionMigrationTest extends HecatonRaftTestSupport {

    @Test
    public void whenPartitionIsMigrated_thenSessionInformationShouldMigrate() {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "2");

        HecatonInstance hz1 = factory.newHecatonInstance(config);
        RaftGroupId group1 = getRaftService(hz1).createRaftGroupForProxy(generateName(hz1, 0));
        RaftGroupId group2 = getRaftService(hz1).createRaftGroupForProxy(generateName(hz1, 1));

        ProxySessionManagerService sessionManager = getSessionManager(hz1);

        long session1 = sessionManager.acquireSession(group1);
        long session2 = sessionManager.acquireSession(group2);

        // Start new instance to trigger migration
        HecatonInstance hz2 = factory.newHecatonInstance(config);
        waitAllForSafeState(hz1, hz2);

        // Unlock after lock is migrated
        sessionManager.heartbeat(group1, session1).joinInternal();
        sessionManager.heartbeat(group2, session2).joinInternal();
    }

    private ProxySessionManagerService getSessionManager(HecatonInstance instance) {
        return getNodeEngineImpl(instance).getService(ProxySessionManagerService.SERVICE_NAME);
    }

    private String generateName(HecatonInstance instance, int partitionId) {
        return "group@" + generateKeyForPartition(instance, partitionId);
    }
}
