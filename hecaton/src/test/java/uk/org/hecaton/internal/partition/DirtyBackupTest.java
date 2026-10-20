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

import java.util.Collection;

import static uk.org.hecaton.test.Accessors.getNode;
import static java.util.Arrays.asList;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DirtyBackupTest extends PartitionCorrectnessTestSupport {

    @Parameters(name = "backups:{0},nodes:{1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {1, 2},
                {2, 3},
                {3, 4},
        });
    }

    @Override
    protected Config getConfig() {
        // Partition count is overwritten back to PartitionCorrectnessTestSupport.partitionCount
        // in PartitionCorrectnessTestSupport.getConfig(boolean, boolean).
        return smallInstanceConfig();
    }

    @Test
    public void testPartitionData_withoutAntiEntropy() {
        startInstancesAndFillPartitions(false);
        assertSizeAndDataEventually(true);
    }

    @Test
    public void testPartitionData_withAntiEntropy() {
        startInstancesAndFillPartitions(true);
        assertSizeAndDataEventually(false);
    }

    private void startInstancesAndFillPartitions(boolean antiEntropyEnabled) {
        backupCount = 1;
        nodeCount = 3;

        HecatonInstance[] instances = factory.newInstances(getConfig(true, antiEntropyEnabled), nodeCount);
        for (HecatonInstance instance : instances) {
            setBackupPacketReorderFilter(instance);
        }
        warmUpPartitions(instances);

        for (HecatonInstance instance : instances) {
            fillData(instance);
        }
    }

    private static void setBackupPacketReorderFilter(HecatonInstance instance) {
        Node node = getNode(instance);
        FirewallingServer.FirewallingServerConnectionManager cm = (FirewallingServer.FirewallingServerConnectionManager)
                node.getServer().getConnectionManager(EndpointQualifier.MEMBER);
        cm.setPacketFilter(new BackupPacketReorderFilter(node.getSerializationService()));
        cm.setDelayMillis(100, 1000);
    }

    private static class BackupPacketReorderFilter extends OperationPacketFilter implements PacketFilter {

        BackupPacketReorderFilter(InternalSerializationService serializationService) {
            super(serializationService);
        }

        @Override
        protected Action filterOperation(Address endpoint, int factory, int type) {
            boolean isBackup = factory == SpiDataSerializerHook.F_ID && type == SpiDataSerializerHook.BACKUP;
            return isBackup ? Action.DELAY : Action.ALLOW;
        }
    }
}
