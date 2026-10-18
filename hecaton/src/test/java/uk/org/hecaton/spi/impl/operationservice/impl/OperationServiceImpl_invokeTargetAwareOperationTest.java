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

package uk.org.hecaton.spi.impl.operationservice.impl;

import uk.org.hecaton.cluster.Address;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.partition.InternalPartition;
import uk.org.hecaton.internal.partition.InternalPartitionService;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.test.HecatonSerialClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getOperationService;
import static uk.org.hecaton.test.Accessors.getPartitionService;
import static org.junit.Assert.assertEquals;

@RunWith(HecatonSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationServiceImpl_invokeTargetAwareOperationTest extends HecatonTestSupport {

    @Rule
    public TestName testName = new TestName();

    private HecatonInstance local;
    private OperationServiceImpl operationService;
    private HecatonInstance remote;

    @Before
    public void setup() {
        int clusterSize;
        if (testName.getMethodName().equals("whenInvokedWithTargetAwareBackups_multipleBackupsHaveTargetInjected")) {
            clusterSize = 5;
        } else {
            clusterSize = 2;
        }
        HecatonInstance[] nodes = createHecatonInstanceFactory(clusterSize).newInstances();
        warmUpPartitions(nodes);

        local = nodes[0];
        remote = nodes[1];
        operationService = getOperationService(local);
        TargetAwareOperation.TARGETS.clear();
    }

    @Test
    public void whenInvokedOnLocalPartition() {
        Address expected = getAddress(local);
        TargetAwareOperation operation = new TargetAwareOperation();

        InternalCompletableFuture<Address> invocation = operationService.invokeOnPartition(
                null, operation, getPartitionId(local));
        Address actual = invocation.join();
        assertEquals(expected, actual);
    }

    @Test
    public void whenInvokedOnRemotePartition() {
        Address expected = getAddress(remote);
        TargetAwareOperation operation = new TargetAwareOperation();

        InternalCompletableFuture<Address> invocation = operationService.invokeOnPartition(
                null, operation, getPartitionId(remote));
        Address actual = invocation.join();
        assertEquals(expected, actual);
    }

    @Test
    public void whenInvokedOnLocalTarget() {
        Address expected = getAddress(local);
        TargetAwareOperation operation = new TargetAwareOperation();

        InternalCompletableFuture<Address> invocation = operationService.invokeOnTarget(
                null, operation, getAddress(local));
        Address actual = invocation.join();
        assertEquals(expected, actual);
    }

    @Test
    public void whenInvokedOnRemoteTarget() {
        Address expected = getAddress(remote);
        TargetAwareOperation operation = new TargetAwareOperation();

        InternalCompletableFuture<Address> invocation = operationService.invokeOnTarget(
                null, operation, getAddress(remote));
        Address actual = invocation.join();
        assertEquals(expected, actual);
    }

    @Test
    public void whenInvokedWithTargetAwareBackup_singleBackupHasTargetInjected() {
        TargetAwareOperation operation = new TargetAwareOperation(1, 0, getPartitionId(remote));

        operationService.invokeOnPartition(null, operation, operation.getPartitionId()).join();

        // primary operation targets remote, backup targets local
        assertEquals(getAddress(remote), TargetAwareOperation.TARGETS.get(0));
        assertEquals(getAddress(local), TargetAwareOperation.TARGETS.get(1));
    }

    @Test
    public void whenInvokedWithTargetAwareBackups_multipleBackupsHaveTargetInjected() {
        int backupCount = 4;
        int partitionId = getPartitionId(local);
        InternalPartitionService localPartitionService = getPartitionService(local);
        InternalPartition partition = localPartitionService.getPartition(partitionId);

        List<Address> expectedTargetAddresses = new ArrayList<>();
        for (int i = 0; i < backupCount + 1; i++) {
            expectedTargetAddresses.add(partition.getReplicaAddress(i));
        }

        TargetAwareOperation operation = new TargetAwareOperation(backupCount, 0, partitionId);

        operationService.invokeOnPartition(null, operation, operation.getPartitionId()).join();
        assertEquals(expectedTargetAddresses, TargetAwareOperation.TARGETS);
    }
}
