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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.spi.impl.CompletableFutureAbstractTest;
import uk.org.hecaton.spi.impl.InternalCompletableFuture;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.test.ExpectedRuntimeException;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletionStage;

import static uk.org.hecaton.test.HecatonTestSupport.smallInstanceConfig;

/**
 * Tests the {@link CompletionStage} implementation of {@link InvocationFuture}.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InvocationFuture_CompletionStageTest extends CompletableFutureAbstractTest {

    private TestHecatonInstanceFactory factory;
    private HecatonInstance local;

    @Before
    public void setup() {
        factory = new TestHecatonInstanceFactory();
        local = factory.newHecatonInstance(getConfig());
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    protected Config getConfig() {
        Config config = smallInstanceConfig();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        return config;
    }

    @Override
    protected InternalCompletableFuture<Object> newCompletableFuture(boolean exceptional, long completeAfterMillis) {
        if (completeAfterMillis <= 0) {
            return invokeSync(local, exceptional);
        } else {
            return invokeAsync(local, exceptional, completeAfterMillis);
        }
    }

    InternalCompletableFuture<Object> invokeSync(HecatonInstance instance, boolean throwsException) {
        return throwsException ? CompletableFutureTestUtil.invokeSync(instance, new Operation() {
            @Override
            public void run() {
                throw new ExpectedRuntimeException();
            }
        }) : CompletableFutureTestUtil.invokeSync(instance, new DummyOperation(returnValue));
    }

    InternalCompletableFuture<Object> invokeAsync(HecatonInstance instance, boolean throwsException,
                                                        long completeAfterMillis) {
        return throwsException ? CompletableFutureTestUtil.invokeAsync(instance, new Operation() {
            @Override
            public void run() {
                throw new ExpectedRuntimeException();
            }
        }) : CompletableFutureTestUtil.invokeAsync(instance, new SlowOperation(completeAfterMillis, returnValue));
    }
}
