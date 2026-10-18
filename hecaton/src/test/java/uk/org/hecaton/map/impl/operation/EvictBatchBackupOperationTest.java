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

package uk.org.hecaton.map.impl.operation;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.map.LocalMapStats;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.spi.impl.operationservice.impl.OperationServiceImpl;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;

import static uk.org.hecaton.test.Accessors.getOperationService;
import static uk.org.hecaton.test.Accessors.getPartitionService;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EvictBatchBackupOperationTest extends HecatonTestSupport {

    TestHecatonInstanceFactory factory = createHecatonInstanceFactory();

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }

    @Test
    public void operation_evicts_all_replicas() {
        // 1. Create config
        int backupCount = 2;
        final String mapName = "test";

        Config config = getConfig();
        config.getMapConfig(mapName).setBackupCount(backupCount);

        // 2. Create nodes
        HecatonInstance node1 = factory.newHecatonInstance(config);
        HecatonInstance node2 = factory.newHecatonInstance(config);
        HecatonInstance node3 = factory.newHecatonInstance(config);

        // 3. Populate replicas
        IMap map = node1.getMap(mapName);
        for (int i = 0; i < 1000; i++) {
            map.set(i, i);
        }

        // 4. Evict all replicas
        int partitionCount = getPartitionService(node1).getPartitionCount();
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            for (int replicaIndex = 0; replicaIndex <= backupCount; replicaIndex++) {
                EvictBatchBackupOperation operation = new EvictBatchBackupOperation(mapName,
                        Collections.emptyList(), 0);
                OperationServiceImpl operationService = getOperationService(node1);
                operationService.createInvocationBuilder(MapService.SERVICE_NAME, operation, partitionId)
                        .setReplicaIndex(replicaIndex).invoke().join();
            }
        }

        // 5. All replicas should be empty in the end.
        for (HecatonInstance node : factory.getAllHecatonInstances()) {
            assertEquals(0, sumOwnedAndBackupEntryCount(node.getMap(mapName).getLocalMapStats()));
        }
    }

    private static long sumOwnedAndBackupEntryCount(LocalMapStats localMapStats) {
        long ownedEntryCount = localMapStats.getOwnedEntryCount();
        long backupEntryCount = localMapStats.getBackupEntryCount();
        return ownedEntryCount + backupEntryCount;
    }
}
