/*
 * Copyright (c) 2026, The Hecaton Authors.
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
package uk.org.hecaton.cp;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.lock.FencedLock;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The CP Subsystem (F7) on three members: the data structures work and agree.
 */
@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPSubsystemSmokeTest extends HecatonTestSupport {

    @Test
    public void dataStructuresWorkAcrossMembers() throws Exception {
        Config config = smallInstanceConfig();
        config.getCPSubsystemConfig().setCPMemberCount(3);
        HecatonInstance[] members = createHecatonInstanceFactory(3).newInstances(config);

        assertThat(members[0].getCPSubsystem().getAtomicLong("counter").incrementAndGet()).isEqualTo(1);
        assertThat(members[1].getCPSubsystem().getAtomicLong("counter").addAndGet(5)).isEqualTo(6);
        assertThat(members[2].getCPSubsystem().getAtomicLong("counter").get()).isEqualTo(6);

        FencedLock lock = members[0].getCPSubsystem().getLock("lock");
        long fence = lock.lockAndGetFence();
        assertThat(members[1].getCPSubsystem().getLock("lock").tryLock()).isFalse();
        lock.unlock();
        assertThat(members[1].getCPSubsystem().getLock("lock").tryLockAndGetFence()).isGreaterThan(fence);

        ISemaphore semaphore = members[2].getCPSubsystem().getSemaphore("permits");
        assertThat(semaphore.init(2)).isTrue();
        assertThat(semaphore.tryAcquire(2)).isTrue();
        assertThat(members[0].getCPSubsystem().getSemaphore("permits").availablePermits()).isZero();

        assertThat(members[0].getCPSubsystem().getCPSubsystemManagementService().getCPMembers()
                .toCompletableFuture().get()).hasSize(3);
    }
}
