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

package uk.org.hecaton.internal.partition.impl;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.instance.impl.Node;
import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.Set;

import static uk.org.hecaton.internal.partition.NonFragmentedServiceNamespace.INSTANCE;
import static uk.org.hecaton.test.Accessors.getNode;
import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionReplicaManagerTest extends HecatonTestSupport {

    private static final int PARTITION_ID = 23;

    private TestHecatonInstanceFactory factory;
    private HecatonInstance hecatonInstance;

    private PartitionReplicaManager manager;

    @Before
    public void setUp() {
        factory = createHecatonInstanceFactory(1);
        hecatonInstance = factory.newHecatonInstance();

        Node node = getNode(hecatonInstance);
        NodeEngineImpl nodeEngine = getNodeEngineImpl(hecatonInstance);
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();

        manager = new PartitionReplicaManager(node, partitionService);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test(expected = AssertionError.class)
    public void testTriggerPartitionReplicaSync_whenReplicaIndexNegative_thenThrowException() {
        Set<ServiceNamespace> namespaces = Collections.singleton(INSTANCE);
        manager.triggerPartitionReplicaSync(PARTITION_ID, namespaces, -1);
    }

    @Test(expected = AssertionError.class)
    public void testTriggerPartitionReplicaSync_whenReplicaIndexTooLarge_thenThrowException() {
        Set<ServiceNamespace> namespaces = Collections.singleton(INSTANCE);
        manager.triggerPartitionReplicaSync(PARTITION_ID, namespaces, InternalPartition.MAX_REPLICA_COUNT + 1);
    }

    @Test
    public void testCheckSyncPartitionTarget_whenPartitionOwnerIsNull_thenReturnFalse() {
        assertNull(manager.checkAndGetPrimaryReplicaOwner(PARTITION_ID, 0));
    }

    @Test
    public void testCheckSyncPartitionTarget_whenNodeIsPartitionOwner_thenReturnFalse() {
        warmUpPartitions(hecatonInstance);

        assertNull(manager.checkAndGetPrimaryReplicaOwner(PARTITION_ID, 0));
    }
}
