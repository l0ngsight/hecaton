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

package uk.org.hecaton.internal.cluster.impl;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.cluster.ClusterState;
import uk.org.hecaton.core.HecatonInstance;
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

import java.util.Arrays;
import java.util.Collection;

import static uk.org.hecaton.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static uk.org.hecaton.test.Accessors.getNode;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterRollingRestartTest extends HecatonTestSupport {

    @Parameters(name = "clusterState:{0},partitionAssignmentType:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {ClusterState.FROZEN, PartitionAssignmentType.NEVER},
                {ClusterState.PASSIVE, PartitionAssignmentType.NEVER},
                {ClusterState.FROZEN, PartitionAssignmentType.AT_THE_END},
                {ClusterState.PASSIVE, PartitionAssignmentType.AT_THE_END},
                {ClusterState.FROZEN, PartitionAssignmentType.DURING_STARTUP},
                {ClusterState.PASSIVE, PartitionAssignmentType.DURING_STARTUP},
        });
    }

    @Parameter(0)
    public ClusterState clusterState;

    @Parameter(1)
    public PartitionAssignmentType partitionAssignmentType;

    @Test
    public void test_rollingRestart() {
        TestHecatonInstanceFactory factory = createHecatonInstanceFactory();
        int nodeCount = 3;
        HecatonInstance[] instances = new HecatonInstance[nodeCount];
        instances[0] = factory.newHecatonInstance();

        if (partitionAssignmentType == PartitionAssignmentType.DURING_STARTUP) {
            warmUpPartitions(instances[0]);
        }

        for (int i = 1; i < nodeCount; i++) {
            instances[i] = factory.newHecatonInstance();
        }

        if (partitionAssignmentType == PartitionAssignmentType.AT_THE_END) {
            warmUpPartitions(instances);
        }

        changeClusterStateEventually(instances[0], clusterState);

        Address address = getNode(instances[0]).getThisAddress();
        instances[0].shutdown();
        instances[0] = factory.newHecatonInstance(address);

        for (HecatonInstance instance : instances) {
            assertClusterSizeEventually(nodeCount, instance);
            assertClusterState(clusterState, instance);
        }

        changeClusterStateEventually(instances[0], ClusterState.ACTIVE);
    }

    private enum PartitionAssignmentType {
        NEVER, DURING_STARTUP, AT_THE_END
    }
}
