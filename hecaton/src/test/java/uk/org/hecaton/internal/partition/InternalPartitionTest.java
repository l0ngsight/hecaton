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
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.Accessors.getNode;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InternalPartitionTest extends HecatonTestSupport {

    @Test
    public void test_isLocal_singleMember() {
        HecatonInstance hz = createHecatonInstance();
        InternalPartition partition = getNode(hz).getPartitionService().getPartition(0);
        assertTrue(partition.isLocal());
    }

    @Test
    public void test_isLocal_multiMember() {
        HecatonInstance[] cluster = createHecatonInstanceFactory(2).newInstances();
        warmUpPartitions(cluster);

        Node node = getNode(cluster[0]);
        InternalPartitionService partitionService = node.getPartitionService();
        Address thisAddress = node.getThisAddress();
        for (int k = 0; k < partitionService.getPartitionCount(); k++) {
            InternalPartition internalPartition = partitionService.getPartition(k);
            if (thisAddress.equals(internalPartition.getOwnerOrNull())) {
                assertTrue("expecting local partition, but found a non local one", internalPartition.isLocal());
            } else {
                assertFalse("expecting non local partition, but found a local one", internalPartition.isLocal());
            }
        }
    }
}
