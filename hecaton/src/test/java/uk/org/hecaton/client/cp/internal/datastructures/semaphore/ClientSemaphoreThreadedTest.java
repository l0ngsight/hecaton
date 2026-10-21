/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package uk.org.hecaton.client.cp.internal.datastructures.semaphore;

import uk.org.hecaton.client.test.TestHecatonFactory;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.ISemaphore;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static uk.org.hecaton.test.HecatonTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientSemaphoreThreadedTest {

    private final TestHecatonFactory hecatonFactory = new TestHecatonFactory();

    private HecatonInstance client;

    @After
    public void tearDown() {
        hecatonFactory.terminateAll();
    }

    @Before
    public void setup() {
        hecatonFactory.newHecatonInstance();
        client = hecatonFactory.newHecatonClient();
    }


    @Test
    public void concurrent_trySemaphoreTest() {
        concurrent_trySemaphoreTest(false);
    }

    @Test
    public void concurrent_trySemaphoreWithTimeOutTest() {
        concurrent_trySemaphoreTest(true);
    }

    public void concurrent_trySemaphoreTest(final boolean tryWithTimeOut) {
        final ISemaphore semaphore = client.getCPSubsystem().getSemaphore(randomString());
        semaphore.init(1);
        final AtomicInteger upTotal = new AtomicInteger(0);
        final AtomicInteger downTotal = new AtomicInteger(0);

        final SemaphoreTestThread[] threads = new SemaphoreTestThread[8];
        for (int i = 0; i < threads.length; i++) {
            SemaphoreTestThread t;
            if (tryWithTimeOut) {
                t = new TrySemaphoreTimeOutThread(semaphore, upTotal, downTotal);
            } else {
                t = new TrySemaphoreThread(semaphore, upTotal, downTotal);
            }
            t.start();
            threads[i] = t;
        }
        HecatonTestSupport.assertJoinable(threads);

        for (SemaphoreTestThread t : threads) {
            assertNull("thread " + t + " has error " + t.error, t.error);
        }

        assertEquals("concurrent access to locked code caused wrong total", 0, upTotal.get() + downTotal.get());
    }

    static class TrySemaphoreThread extends SemaphoreTestThread {
        TrySemaphoreThread(ISemaphore semaphore, AtomicInteger upTotal, AtomicInteger downTotal) {
            super(semaphore, upTotal, downTotal);
        }

        public void iterativelyRun() throws Exception {
            if (semaphore.tryAcquire()) {
                work();
                semaphore.release();
            }
        }
    }

    static class TrySemaphoreTimeOutThread extends SemaphoreTestThread {
        TrySemaphoreTimeOutThread(ISemaphore semaphore, AtomicInteger upTotal, AtomicInteger downTotal) {
            super(semaphore, upTotal, downTotal);
        }

        public void iterativelyRun() throws Exception {
            if (semaphore.tryAcquire(1, TimeUnit.MILLISECONDS)) {
                work();
                semaphore.release();
            }
        }
    }

    abstract static class SemaphoreTestThread extends Thread {

        private static final int MAX_ITTERATIONS = 1000 * 10;

        public volatile Throwable error;

        protected final ISemaphore semaphore;
        protected final AtomicInteger upTotal;
        protected final AtomicInteger downTotal;

        private final Random random = new Random();

        SemaphoreTestThread(ISemaphore semaphore, AtomicInteger upTotal, AtomicInteger downTotal) {
            this.semaphore = semaphore;
            this.upTotal = upTotal;
            this.downTotal = downTotal;
        }

        public final void run() {
            try {
                for (int i = 0; i < MAX_ITTERATIONS; i++) {
                    iterativelyRun();
                }
            } catch (Throwable e) {
                error = e;
            }
        }

        abstract void iterativelyRun() throws Exception;

        protected void work() {
            final int delta = random.nextInt(1000);
            upTotal.addAndGet(delta);
            downTotal.addAndGet(-delta);
        }
    }
}
