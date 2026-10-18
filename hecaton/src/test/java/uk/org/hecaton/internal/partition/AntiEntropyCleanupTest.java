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

package uk.org.hecaton.internal.partition;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.map.impl.MapService;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static uk.org.hecaton.spi.properties.ClusterProperty.PARTITION_BACKUP_SYNC_INTERVAL;
import static uk.org.hecaton.test.Accessors.getPartitionService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertNotEquals;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AntiEntropyCleanupTest extends HecatonTestSupport {

    @Parameters(name = "nodeCount:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {1},
                {2},
        });
    }

    @Parameter
    public int nodeCount;

    @Test
    public void testCleanup() {
        Config config = new Config().setProperty(PARTITION_BACKUP_SYNC_INTERVAL.getName(), "1");
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(nodeCount);
        HecatonInstance[] instances = factory.newInstances(config, nodeCount);
        warmUpPartitions(instances);

        String mapName = randomMapName();

        HecatonInstance instance1 = instances[0];
        for (int partitionId = 0; partitionId < getPartitionService(instance1).getPartitionCount(); partitionId++) {
            String key = generateKeyForPartition(instance1, partitionId);
            instance1.getMap(mapName).put(key, key);
        }

        instance1.getMap(mapName).destroy();

        for (final HecatonInstance instance : instances) {
            final InternalPartitionService partitionService = getPartitionService(instance);
            final PartitionReplicaVersionManager replicaVersionManager = partitionService.getPartitionReplicaVersionManager();

            assertTrueEventually(() -> {
                for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
                    for (ServiceNamespace namespace : replicaVersionManager.getNamespaces(partitionId)) {
                        assertNotEquals(MapService.SERVICE_NAME, namespace.getServiceName());
                    }
                }
            });
        }
    }

}
