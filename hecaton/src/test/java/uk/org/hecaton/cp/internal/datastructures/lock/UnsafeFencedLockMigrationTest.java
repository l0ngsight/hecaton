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

import uk.org.hecaton.config.Config;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.cp.internal.HecatonRaftTestSupport;
import uk.org.hecaton.cp.lock.FencedLock;
import uk.org.hecaton.spi.properties.ClusterProperty;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnsafeFencedLockMigrationTest extends HecatonRaftTestSupport {

    @Test
    public void whenLockIsMigrated_thenSessionInformationShouldMigrate() {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "2");

        HecatonInstance hz1 = factory.newHecatonInstance(config);

        // Create two locks on each partition,
        // so one of them is guaranteed to be migrated
        // after new instance is started.
        FencedLock lock1 = hz1.getCPSubsystem().getLock(generateName(hz1, 0));
        FencedLock lock2 = hz1.getCPSubsystem().getLock(generateName(hz1, 1));

        lock1.lock();
        lock2.lock();

        // Start new instance to trigger migration
        HecatonInstance hz2 = factory.newHecatonInstance(config);
        waitAllForSafeState(hz1, hz2);

        // Unlock after lock is migrated
        lock1.unlock();
        lock2.unlock();
    }

    @Test
    public void whenLockIsMigrated_thenWaitingOpsShouldBeNotified() throws Exception {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "2");

        HecatonInstance hz1 = factory.newHecatonInstance(config);

        // Create two locks on each partition,
        // so one of them is guaranteed to be migrated
        // after new instance is started.
        FencedLock lock1 = hz1.getCPSubsystem().getLock(generateName(hz1, 0));
        FencedLock lock2 = hz1.getCPSubsystem().getLock(generateName(hz1, 1));

        lock1.lock();
        lock2.lock();

        Future waitingLock1 = spawn(lock1::lock);
        Future waitingLock2 = spawn(lock2::lock);

        // Ensure waiting op is registered
        LockService lockService = getNodeEngineImpl(hz1).getService(LockService.SERVICE_NAME);
        assertTrueEventually(() -> {
            LockRegistry registry1 = lockService.getRegistryOrNull(lock1.getGroupId());
            assertThat(registry1.getLiveOperations()).hasSize(1);

            LockRegistry registry2 = lockService.getRegistryOrNull(lock2.getGroupId());
            assertThat(registry2.getLiveOperations()).hasSize(1);
        });

        // Start new instance to trigger migration
        HecatonInstance hz2 = factory.newHecatonInstance(config);
        waitAllForSafeState(hz1, hz2);

        // Unlock after lock is migrated
        lock1.unlock();
        lock2.unlock();

        // Waiting op is triggered after unlock
        waitingLock1.get();
        waitingLock2.get();
    }

    @Test
    public void whenLockIsBlocked_thenBackupIsReplicated() {
        Config config = new Config();

        HecatonInstance hz1 = factory.newHecatonInstance(config);
        HecatonInstance hz2 = factory.newHecatonInstance(config);

        FencedLock lock = hz2.getCPSubsystem().getLock("lock@" + generateKeyOwnedBy(hz1));
        CountDownLatch latch = new CountDownLatch(1);

        spawn(() -> {
            lock.lock();
            latch.countDown();

            // wait until other thread blocks
            LockService lockService = getNodeEngineImpl(hz1).getService(LockService.SERVICE_NAME);
            assertTrueEventually(() -> {
                LockRegistry registry = lockService.getRegistryOrNull(lock.getGroupId());
                assertThat(registry.getLiveOperations()).hasSize(1);
            });
            lock.unlock();
        });

        assertOpenEventually(latch);
        lock.lock();

        waitAllForSafeState(hz1, hz2);
        hz1.getLifecycleService().terminate();

        lock.unlock();
    }

    private String generateName(HecatonInstance instance, int partitionId) {
        return "lock@" + generateKeyForPartition(instance, partitionId);
    }

}
