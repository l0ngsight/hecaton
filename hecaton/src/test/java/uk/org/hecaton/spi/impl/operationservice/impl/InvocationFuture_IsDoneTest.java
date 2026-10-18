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
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getOperationService;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InvocationFuture_IsDoneTest extends HecatonTestSupport {

    private HecatonInstance local;
    private OperationServiceImpl operationService;

    @Before
    public void setup() {
        local = createHecatonInstance();
        operationService = getOperationService(local);
    }

    @Test
    public void whenNullResponse() throws ExecutionException, InterruptedException {
        DummyOperation op = new DummyOperation(null);

        InternalCompletableFuture<Object> future = operationService.invokeOnTarget(null, op, getAddress(local));
        future.get();

        assertTrue(future.isDone());
    }

    @Test
    public void whenInterruptedResponse() {
        DummyOperation op = new GetLostPartitionOperation();

        InvocationFuture<Object> future = operationService.invokeOnTarget(null, op, getAddress(local));
        future.complete(InvocationConstant.INTERRUPTED);

        assertTrue(future.isDone());
    }

    @Test
    public void whenTimeoutResponse() {
        DummyOperation op = new GetLostPartitionOperation();

        InvocationFuture<Object> future = operationService.invokeOnTarget(null, op, getAddress(local));
        future.complete(InvocationConstant.CALL_TIMEOUT);

        assertTrue(future.isDone());
    }

    @Test
    public void isDone_whenNoResponse() {
        DummyOperation op = new GetLostPartitionOperation();

        InternalCompletableFuture<Object> future = operationService.invokeOnTarget(null, op, getAddress(local));

        assertFalse(future.isDone());
    }

    @Test
    public void isDone_whenObjectResponse() {
        DummyOperation op = new DummyOperation("foobar");

        InternalCompletableFuture<Object> future = operationService.invokeOnTarget(null, op, getAddress(local));

        assertTrue(future.isDone());
    }

    // needed to have an invocation and this is the easiest way how to get one and do not bother with its result
    private static class GetLostPartitionOperation extends DummyOperation {
        {
            // we need to set the call ID to prevent running the operation on the calling-thread
            setPartitionId(1);
        }

        @Override
        public void run() throws Exception {
            Thread.sleep(5000);
        }
    }
}
