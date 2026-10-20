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

package uk.org.hecaton.ringbuffer.impl;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RingbufferParkingTest extends HecatonTestSupport {
    private static final String RINGBUFFER_NAME = "ringbuffer";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private HecatonInstance instance;

    @Before
    public void setup() {
        instance = createHecatonInstance();
    }

    @Test
    public void multipleReadManyOperationsDoNotBlockEachOther() throws ExecutionException, InterruptedException, TimeoutException {
        var ringbuffer = instance.getRingbuffer(RINGBUFFER_NAME);
        // In this test, operations run locally, so we can be sure that the first reader
        // operation is added to the partition queue before the second reader,
        // which ensures the first operation will be parked before the second one.
        var firstReader = ringbuffer.readManyAsync(0, 10, 100, e -> true);
        var secondReader = ringbuffer.readManyAsync(0, 5, 100, e -> true);

        assertThat(firstReader).isNotCompleted();
        assertThat(secondReader).isNotCompleted();

        // Add a specific number of items to unblock the second reader, but not the first reader.
        for (int i = 0; i < 5; i++) {
            ringbuffer.add(i);
        }

        secondReader.toCompletableFuture().get(TIMEOUT.toSeconds(), SECONDS);
        assertThat(firstReader).isNotCompleted();
    }
}
