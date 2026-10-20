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

package uk.org.hecaton.client.impl.spi.impl;

import uk.org.hecaton.client.impl.clientside.ClientLoggingService;
import uk.org.hecaton.config.Config;
import uk.org.hecaton.instance.BuildInfoProvider;
import uk.org.hecaton.spi.properties.HecatonProperties;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientExecutionServiceImplTest {

    private static ClientExecutionServiceImpl executionService;

    @BeforeClass
    public static void setUp() {
        String name = "ClientExecutionServiceImplTest";
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        HecatonProperties properties = new HecatonProperties(new Config());
        ClientLoggingService loggingService = new ClientLoggingService(
                name, "jdk", BuildInfoProvider.getBuildInfo(), name, true, false
        );

        executionService = new ClientExecutionServiceImpl(name, classLoader, properties, loggingService);
    }

    @AfterClass
    public static void tearDown() {
        executionService.shutdown();
    }

    @Test
    public void testExecuteInternal() {
        TestRunnable runnable = new TestRunnable();

        executionService.execute(runnable);

        runnable.await();
    }

    @Test
    public void testSchedule() throws Exception {
        TestRunnable runnable = new TestRunnable();

        ScheduledFuture<?> future = executionService.schedule(runnable, 0, SECONDS);
        Object result = future.get();

        assertTrue(runnable.isExecuted());
        assertNull(result);
    }

    @Test
    public void testScheduleWithRepetition() {
        TestRunnable runnable = new TestRunnable(5);

        ScheduledFuture<?> future = executionService.scheduleWithRepetition(runnable, 0, 100, MILLISECONDS);
        runnable.await();

        boolean result = future.cancel(true);
        assertTrue(result);
    }

    private static class TestRunnable implements Runnable {

        private final CountDownLatch isExecuted;

        TestRunnable() {
            this(1);
        }

        TestRunnable(int executions) {
            this.isExecuted = new CountDownLatch(executions);
        }

        @Override
        public void run() {
            isExecuted.countDown();
        }

        private boolean isExecuted() {
            return isExecuted.getCount() == 0;
        }

        private void await() {
            try {
                isExecuted.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
