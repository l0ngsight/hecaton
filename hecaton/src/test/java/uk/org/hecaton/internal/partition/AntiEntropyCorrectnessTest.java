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

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.EndpointQualifier;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.partition.impl.InternalPartitionServiceImpl;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.server.FirewallingServer;
import uk.org.hecaton.internal.server.OperationPacketFilter;
import uk.org.hecaton.internal.server.PacketFilter;
import uk.org.hecaton.spi.impl.SpiDataSerializerHook;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AntiEntropyCorrectnessTest extends PartitionCorrectnessTestSupport {

    private static final float BACKUP_BLOCK_RATIO = 0.65f;

    @Parameters(name = "backups:{0},nodes:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {1, 2},
                {1, InternalPartition.MAX_REPLICA_COUNT},
                {2, 3},
                {2, InternalPartition.MAX_REPLICA_COUNT},
                {3, 4},
                {3, InternalPartition.MAX_REPLICA_COUNT},
        });
    }

    @Override
    protected Config getConfig() {
        // Partition count is overwritten back to PartitionCorrectnessTestSupport.partitionCount
        // in PartitionCorrectnessTestSupport.getConfig(boolean, boolean).
        return smallInstanceConfig();
    }

    @Test
    public void testPartitionData() {
        final HecatonInstance[] instances = factory.newInstances(getConfig(true, true), nodeCount);
        for (HecatonInstance instance : instances) {
            setBackupPacketDropFilter(instance, BACKUP_BLOCK_RATIO);
        }
        warmUpPartitions(instances);

        for (HecatonInstance instance : instances) {
            fillData(instance);
        }

        assertSizeAndDataEventually();

        assertTrueEventually(() -> {
            for (HecatonInstance instance : instances) {
                InternalPartitionServiceImpl partitionService = getNode(instance).partitionService;
                int availablePermits = partitionService.getReplicaManager().availableReplicaSyncPermits();
                assertEquals(PARALLEL_REPLICATIONS, availablePermits);
            }
        }, 30);
    }

    public static void setBackupPacketDropFilter(HecatonInstance instance, float blockRatio) {
        Node node = getNode(instance);
        FirewallingServer.FirewallingServerConnectionManager cm = (FirewallingServer.FirewallingServerConnectionManager)
                node.getServer().getConnectionManager(EndpointQualifier.MEMBER);
        cm.setPacketFilter(new BackupPacketDropFilter(node.getSerializationService(), blockRatio));
    }

    private static class BackupPacketDropFilter extends OperationPacketFilter implements PacketFilter {
        final float blockRatio;

        BackupPacketDropFilter(InternalSerializationService serializationService, float blockRatio) {
            super(serializationService);
            this.blockRatio = blockRatio;
        }

        @Override
        protected Action filterOperation(Address endpoint, int factory, int type) {
            boolean isBackup = factory == SpiDataSerializerHook.F_ID && type == SpiDataSerializerHook.BACKUP;
            return !isBackup ? Action.ALLOW : (Math.random() > blockRatio ? Action.ALLOW : Action.DROP);
        }
    }
}
