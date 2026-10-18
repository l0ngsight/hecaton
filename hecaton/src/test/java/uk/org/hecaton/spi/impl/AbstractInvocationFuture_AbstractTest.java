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

package uk.org.hecaton.spi.impl;

import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.test.HecatonTestSupport;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static uk.org.hecaton.spi.impl.operationservice.impl.InvocationFuture.returnOrThrowWithGetConventions;

public abstract class AbstractInvocationFuture_AbstractTest extends HecatonTestSupport {

    protected ILogger logger;
    protected ExecutorService executor;
    protected TestFuture future;
    protected Object value = "somevalue";

    @Before
    public void setup() {
        logger = Logger.getLogger(getClass());
        executor = Executors.newSingleThreadExecutor();
        future = new TestFuture();
    }

    @After
    public void tearDown() {
        executor.shutdownNow();
    }

    class TestFuture extends AbstractInvocationFuture {
        volatile boolean interruptDetected;

        private final Executor executor;

        TestFuture() {
            super(AbstractInvocationFuture_AbstractTest.this.logger);
            this.executor = AbstractInvocationFuture_AbstractTest.this.executor;
        }

        TestFuture(Executor executor, ILogger logger) {
            super(logger);
            this.executor = executor;
        }

        @Override
        public Executor defaultExecutor() {
            return executor;
        }

        @Override
        protected void onInterruptDetected() {
            interruptDetected = true;
            completeExceptionally(new InterruptedException());
        }

        @Override
        protected IllegalStateException wrapToInstanceNotActiveException(RejectedExecutionException e) {
            return new HecatonInstanceNotActiveException(e.getMessage());
        }

        @Override
        protected String invocationToString() {
            return "someinvocation";
        }

        @Override
        protected Object resolveAndThrowIfException(Object state) throws ExecutionException, InterruptedException {
            Object value = resolve(state);
            return returnOrThrowWithGetConventions(value);
        }

        @Override
        protected TimeoutException newTimeoutException(long timeout, TimeUnit unit) {
            return new TimeoutException();
        }
    }
}
