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

import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static uk.org.hecaton.test.Accessors.getOperationService;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class Invocation_TaskDoneTest extends HecatonTestSupport {

    private OperationServiceImpl operationService;

    @Before
    public void before() {
        operationService = getOperationService(createHecatonInstance());
    }

    @Test
    public void when_invocationDone_thenCallbackRuns() {
        // Given
        final DummyOperation op = new DummyOperation(null);
        final DoneCallback cb = new DoneCallback();

        // When
        operationService.createInvocationBuilder("mockService", op, 0).setDoneCallback(cb).invoke();

        // Then
        assertTrueEventually(() -> assertTrue(cb.done));
    }

    @Test
    public void when_invocationFutureCanceled_thenCallbackRunsEventually() {
        // Given
        final LatchAwaitOperation latchAwaitOp = new LatchAwaitOperation();
        final DoneCallback cb = new DoneCallback();
        final InternalCompletableFuture<Object> fut =
                operationService.createInvocationBuilder("mockService", latchAwaitOp, 0).setDoneCallback(cb).invoke();
        final FailedLatchExecutionCallback canceledCallback = new FailedLatchExecutionCallback();
        fut.exceptionally(canceledCallback);

        // When
        fut.cancel(true);
        assertOpenEventually(canceledCallback.latch);

        // Then
        assertFalse(cb.done);
        latchAwaitOp.latch.countDown();
        assertTrueEventually(() -> assertTrue(cb.done));
    }

    static class DoneCallback implements Runnable {
        volatile boolean done;

        @Override
        public void run() {
            done = true;
        }
    }

    static class LatchAwaitOperation extends Operation {
        final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public void run() throws Exception {
            latch.await();
        }
    }

    static class FailedLatchExecutionCallback implements Function<Throwable, Object> {
        final CountDownLatch latch = new CountDownLatch(1);

        @Override
        public Object apply(Throwable throwable) {
            latch.countDown();
            return null;
        }
    }
}
