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
import uk.org.hecaton.spi.exception.TargetNotMemberException;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.operationservice.ExceptionAction;
import uk.org.hecaton.test.ExceptionThrowingCallable;
import uk.org.hecaton.test.ExpectedRuntimeException;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.test.Accessors.getAddress;
import static uk.org.hecaton.test.Accessors.getOperationService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationServiceImpl_invokeOnTargetTest extends HecatonTestSupport {

    private HecatonInstance local;
    private OperationServiceImpl operationService;
    private HecatonInstance remote;

    @Before
    public void setup() {
        HecatonInstance[] nodes = createHecatonInstanceFactory(2).newInstances();
        warmUpPartitions(nodes);

        local = nodes[0];
        remote = nodes[1];
        operationService = getOperationService(local);
    }

    @Test
    public void whenLocal() {
        String expected = "foobar";
        DummyOperation operation = new DummyOperation(expected);
        InternalCompletableFuture<String> invocation = operationService.invokeOnTarget(
                null, operation, getAddress(local));
        assertEquals(expected, invocation.join());

        //todo: we need to verify that the call was run on the calling thread
    }

    @Test
    public void whenRemote() {
        String expected = "foobar";
        DummyOperation operation = new DummyOperation(expected);
        InternalCompletableFuture<String> invocation = operationService.invokeOnTarget(
                null, operation, getAddress(remote));
        assertEquals(expected, invocation.join());
    }

    @Test
    public void whenNonExistingTarget() {
        Address remoteAddress = getAddress(remote);
        remote.shutdown();

        // ensure local instance observes remote shutdown
        assertClusterSizeEventually(1, local);

        String expected = "foobar";
        DummyOperation operation = new DummyOperation(expected) {
            @Override
            public ExceptionAction onInvocationException(Throwable throwable) {
                // Don't retry when TargetNotMemberException is received.
                // Invocation is registered before checking target. If invocation is retried
                // when TargetNotMemberException is get, invocation may fail with MemberLeftException too.
                if (throwable instanceof TargetNotMemberException) {
                    return ExceptionAction.THROW_EXCEPTION;
                }
                return super.onInvocationException(throwable);
            }
        };

        InternalCompletableFuture<String> invocation = operationService.invokeOnTarget(
                null, operation, remoteAddress);

        try {
            invocation.joinInternal();
            fail();
        } catch (TargetNotMemberException e) {
        }
    }

    @Test
    public void whenExceptionThrownInOperationRun() {
        DummyOperation operation = new DummyOperation(new ExceptionThrowingCallable());
        InternalCompletableFuture<String> invocation = operationService.invokeOnTarget(
                null, operation, getAddress(remote));

        try {
            invocation.joinInternal();
            fail();
        } catch (ExpectedRuntimeException expected) {
        }
    }

}
