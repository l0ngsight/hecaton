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
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CancellationException;

import static uk.org.hecaton.test.Accessors.getOperationService;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InvocationFuture_CancelTest extends HecatonTestSupport {

    private static final int RESULT = 123;


    private OperationServiceImpl opService;

    @Before
    public void setup() {
        HecatonInstance hz = createHecatonInstance();
        opService = getOperationService(hz);
    }

    @Test
    public void whenCallCancel_thenCancelled() {
        // Given
        InternalCompletableFuture<Object> future = invoke();

        // When
        boolean result = future.cancel(true);

        // Then
        assertTrue(result);
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
    }

    @Test
    public void whenCancelled_thenCantCancelAgain() {
        // Given
        InternalCompletableFuture<Object> future = invoke();

        // When
        future.cancel(true);

        // Then
        assertFalse(future.cancel(true));
    }

    @Test
    public void whenCancelled_thenGetThrowsCancelled() {
        // Given
        InternalCompletableFuture<Object> future = invoke();

        // When
        future.cancel(true);

        // Then
        assertThrows(CancellationException.class, future::get);
    }

    private InternalCompletableFuture<Object> invoke() {
        Operation op = new Operation() {
            @Override
            public void run() {
                sleepMillis(1000);
            }

            @Override
            public Object getResponse() {
                return RESULT;
            }
        };
        return opService.invokeOnPartition(null, op, 0);
    }
}
