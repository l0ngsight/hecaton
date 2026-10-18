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

package uk.org.hecaton.spi.impl.operationparker.impl;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.map.IMap;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;

import static uk.org.hecaton.test.Accessors.getNode;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationParkerImplTest extends HecatonTestSupport {

    private HecatonInstance hz;
    private OperationParkerImpl waitNotifyService;

    @Before
    public void setup() {
        hz = createHecatonInstance();
        NodeEngineImpl nodeEngine = getNode(hz).nodeEngine;
        waitNotifyService = (OperationParkerImpl) nodeEngine.getOperationParker();
    }

    @Test
    public void testAwaitQueueCount_shouldNotExceedBlockedThreadCount() {
        final int keyCount = 1000;
        int nThreads = 4;
        CountDownLatch latch = new CountDownLatch(nThreads);

        for (int i = 0; i < nThreads; i++) {
            new Thread(new LockWaitAndUnlockTask(hz, keyCount, latch)).start();
        }

        while (latch.getCount() > 0) {
            LockSupport.parkNanos(1);
            int awaitQueueCount = waitNotifyService.getParkQueueCount();
            Assert.assertTrue(
                    "Await queue count should be smaller than total number of threads: " + awaitQueueCount + " VS "
                            + nThreads, awaitQueueCount < nThreads);
        }
    }

    private static class LockWaitAndUnlockTask implements Runnable {
        private final HecatonInstance hz;
        private final int keyCount;
        private final CountDownLatch latch;

        LockWaitAndUnlockTask(HecatonInstance hz, int keyCount, CountDownLatch latch) {
            this.hz = hz;
            this.keyCount = keyCount;
            this.latch = latch;
        }

        @Override
        public void run() {
            for (int i = 0; i < keyCount; i++) {
                IMap<Object, Object> map = hz.getMap("map");
                try {
                    String key = "key" + i;
                    map.lock(key);
                    LockSupport.parkNanos(1);
                    map.unlock(key);
                } catch (HecatonInstanceNotActiveException ignored) {
                }
            }
            latch.countDown();
        }
    }
}
