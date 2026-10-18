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
package uk.org.hecaton.map;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.internal.partition.PartitionReplica;
import uk.org.hecaton.internal.util.UuidUtil;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.map.impl.MapServiceContext;
import uk.org.hecaton.map.impl.recordstore.RecordStore;
import uk.org.hecaton.test.Accessors;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;

import static uk.org.hecaton.test.Accessors.getPartitionService;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapMigrationRecordStoreExistenceTest extends HecatonTestSupport {

    @Test
    public void testRecordStoreExistence() {
        String mapName = UuidUtil.newUnsecureUuidString();
        Config config = getConfig();
        config.addMapConfig(new MapConfig(mapName)
                .setBackupCount(1)
                .setAsyncBackupCount(0));

        // setup cluster
        TestHecatonInstanceFactory nodeFactory = createHecatonInstanceFactory(3);
        HecatonInstance instance1 = nodeFactory.newHecatonInstance(config);
        HecatonInstance instance2 = nodeFactory.newHecatonInstance(config);
        int partitionCount = getPartitionService(instance1).getPartitionCount();

        waitAllForSafeState(instance1, instance2);

        // populate map to create the record stores
        Map<Integer, Integer> map = instance1.getMap(mapName);
        for (int i = 0; i < 10000; i++) {
            map.put(i, i);
        }

        // add instance3 to get some partitions removed from every member
        HecatonInstance instance3 = nodeFactory.newHecatonInstance(config);

        waitAllForSafeState(instance1, instance2, instance3);

        try {
            for (HecatonInstance instance : new HecatonInstance[]{instance1, instance2, instance3}) {
                Address instanceAddress = Accessors.getAddress(instance);
                InternalPartitionService partitionService = getPartitionService(instance);

                MapService mapService = Accessors.getNodeEngineImpl(instance).getService(MapService.SERVICE_NAME);
                MapServiceContext mapServiceContext = mapService.getMapServiceContext();

                for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                    InternalPartition partition = partitionService.getPartition(partitionId);

                    PartitionReplica primaryReplica = partition.getReplica(0);
                    PartitionReplica backupReplica = partition.getReplica(1);

                    boolean primary = primaryReplica.address().equals(instanceAddress);
                    boolean backup = backupReplica.address().equals(instanceAddress);

                    RecordStore<?> recordStore = mapServiceContext.getExistingRecordStore(partitionId, mapName);

                    if (primary || backup) {
                        assertNotNull("Address/partition " + instanceAddress + "/" + partitionId, recordStore);
                    } else {
                        assertNull("Address/partition " + instanceAddress + "/" + partitionId, recordStore);
                    }
                }
            }
        } finally {
            nodeFactory.terminateAll();
        }
    }
}
