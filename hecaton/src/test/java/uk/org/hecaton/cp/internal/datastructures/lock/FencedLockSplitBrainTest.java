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

package uk.org.hecaton.cp.internal.datastructures.lock;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.core.OperationTimeoutException;
import uk.org.hecaton.cp.internal.RaftSplitBrainTestSupport;
import uk.org.hecaton.cp.lock.FencedLock;
import uk.org.hecaton.cp.lock.exception.LockOwnershipLostException;
import uk.org.hecaton.test.HecatonParallelParametersRunnerFactory;
import uk.org.hecaton.test.HecatonParametrizedRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.org.hecaton.cp.internal.HecatonRaftTestSupport.waitUntilCPDiscoveryCompleted;
import static java.util.concurrent.locks.LockSupport.parkNanos;

@RunWith(HecatonParametrizedRunner.class)
@UseParametersRunnerFactory(HecatonParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FencedLockSplitBrainTest extends RaftSplitBrainTestSupport {

    private final String name = "lock";
    private final AtomicBoolean done = new AtomicBoolean();
    private Future[] futures;

    @Override
    protected void onBeforeSplitBrainCreated(HecatonInstance[] instances) {
        waitUntilCPDiscoveryCompleted(instances);

        futures = new Future[instances.length];
        for (int i = 0; i < instances.length; i++) {
            futures[i] = spawn(new Locker(instances[i]));
        }
        sleepSeconds(3);
    }

    @Override
    protected void onAfterSplitBrainCreated(HecatonInstance[] firstBrain, HecatonInstance[] secondBrain) {
        sleepSeconds(withSessionTimeout ? 10 : 5);
    }

    @Override
    protected void onAfterSplitBrainHealed(HecatonInstance[] instances) throws Exception {
        sleepSeconds(3);
        done.set(true);
        for (Future future : futures) {
            assertCompletesEventually(future);
            future.get();
        }
    }

    private class Locker implements Runnable {
        private final HecatonInstance instance;

        Locker(HecatonInstance instance) {
            this.instance = instance;
        }

        @Override
        public void run() {
            FencedLock lock = instance.getCPSubsystem().getLock(name);
            while (!done.get()) {
                try {
                    lock.lock();
                    parkNanos(1);
                    lock.unlock();
                } catch (LockOwnershipLostException e) {
                    e.printStackTrace();
                } catch (OperationTimeoutException ignored) {
                }
            }
        }
    }
}
