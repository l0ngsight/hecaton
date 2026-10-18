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

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletionException;

import static uk.org.hecaton.internal.util.RootCauseMatcher.rootCause;
import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getOperationService;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_NestedRemoteTest extends Invocation_NestedAbstractTest {

    private static final String RESPONSE = "someresponse";

    @Test
    public void invokeOnPartition_outerGeneric_innerGeneric_forbidden() {
        HecatonInstance[] cluster = createHecatonInstanceFactory(1).newInstances();
        HecatonInstance local = cluster[0];
        OperationService operationService = getOperationService(local);

        InnerOperation innerOperation = new InnerOperation(RESPONSE, GENERIC_OPERATION);
        OuterOperation outerOperation = new OuterOperation(innerOperation, GENERIC_OPERATION);

        assertThatThrownBy(() -> {
            InternalCompletableFuture<Object> future =
                    operationService.invokeOnPartition(null, outerOperation, outerOperation.getPartitionId());
            future.join();
        }).isInstanceOf(Exception.class);
    }

    @Test
    public void invokeOnPartition_outerRemote_innerGeneric() {
        HecatonInstance[] cluster = createHecatonInstanceFactory(2).newInstances();
        HecatonInstance local = cluster[0];
        HecatonInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        int partitionId = getPartitionId(remote);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, GENERIC_OPERATION);
        OuterOperation outerOperation = new OuterOperation(innerOperation, partitionId);
        InternalCompletableFuture<Object> future = operationService.invokeOnPartition(null, outerOperation, partitionId);

        assertEquals(RESPONSE, future.join());
    }

    @Test
    public void invokeOnPartition_outerRemote_innerSameInstance_samePartition() {
        HecatonInstance[] cluster = createHecatonInstanceFactory(2).newInstances();
        HecatonInstance local = cluster[0];
        HecatonInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        int partitionId = getPartitionId(remote);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, partitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, partitionId);
        InternalCompletableFuture<Object> future = operationService.invokeOnPartition(null, outerOperation, partitionId);

        assertEquals(RESPONSE, future.join());
    }

    @Test
    public void invokeOnPartition_outerRemote_innerSameInstance_callsDifferentPartition_mappedToSameThread() {
        HecatonInstance[] cluster = createHecatonInstanceFactory(2).newInstances();
        HecatonInstance local = cluster[0];
        HecatonInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        int outerPartitionId = getPartitionId(remote);
        int innerPartitionId = randomPartitionIdMappedToSameThreadAsGivenPartitionIdOnInstance(outerPartitionId, remote,
                operationService);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, innerPartitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, outerPartitionId);
        InternalCompletableFuture<Object> future = operationService.invokeOnPartition(null, outerOperation, outerPartitionId);

        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .has(rootCause(IllegalThreadStateException.class))
                .hasMessageContaining("cannot make remote call");
    }

    @Test
    public void invokeOnPartition_outerRemote_innerDifferentInstance_forbidden() {
        HecatonInstance[] cluster = createHecatonInstanceFactory(2).newInstances();
        HecatonInstance local = cluster[0];
        HecatonInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        int outerPartitionId = getPartitionId(remote);
        int innerPartitionId = getPartitionId(local);
        assertNotEquals("partitions should be different", innerPartitionId, outerPartitionId);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, innerPartitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, outerPartitionId);
        InternalCompletableFuture<Object> future = operationService.invokeOnPartition(null, outerOperation, outerPartitionId);

        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .has(rootCause(IllegalThreadStateException.class))
                .hasMessageContaining("cannot make remote call");
    }

    @Test
    public void invokeOnPartition_outerLocal_innerDifferentInstance_forbidden() {
        HecatonInstance[] cluster = createHecatonInstanceFactory(2).newInstances();
        HecatonInstance local = cluster[0];
        HecatonInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        int outerPartitionId = getPartitionId(local);
        int innerPartitionId = getPartitionId(remote);
        assertNotEquals("partitions should be different", innerPartitionId, outerPartitionId);
        InnerOperation innerOperation = new InnerOperation(RESPONSE, innerPartitionId);
        OuterOperation outerOperation = new OuterOperation(innerOperation, outerPartitionId);
        InternalCompletableFuture<Object> future = operationService.invokeOnPartition(null, outerOperation, outerPartitionId);

        assertThatThrownBy(future::join)
                .isInstanceOf(CompletionException.class)
                .has(rootCause(IllegalThreadStateException.class))
                .hasMessageContaining("cannot make remote call");
    }

    @Test
    public void invokeOnTarget_outerGeneric_innerGeneric() {
        HecatonInstance[] cluster = createHecatonInstanceFactory(2).newInstances();
        HecatonInstance local = cluster[0];
        HecatonInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        InnerOperation innerOperation = new InnerOperation(RESPONSE, GENERIC_OPERATION);
        OuterOperation outerOperation = new OuterOperation(innerOperation, GENERIC_OPERATION);
        InternalCompletableFuture<Object> future = operationService.invokeOnTarget(null, outerOperation, getAddress(remote));

        assertEquals(RESPONSE, future.join());
    }

    @Test
    public void invokeOnTarget_outerGeneric_innerSameInstance() {
        HecatonInstance[] cluster = createHecatonInstanceFactory(2).newInstances();
        HecatonInstance local = cluster[0];
        HecatonInstance remote = cluster[1];
        OperationService operationService = getOperationService(local);

        InnerOperation innerOperation = new InnerOperation(RESPONSE, 0);
        OuterOperation outerOperation = new OuterOperation(innerOperation, GENERIC_OPERATION);
        InternalCompletableFuture<Object> future = operationService.invokeOnTarget(null, outerOperation, getAddress(remote));

        assertEquals(RESPONSE, future.join());
    }
}
