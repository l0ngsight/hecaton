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

package uk.org.hecaton.multimap;

import uk.org.hecaton.config.Config;
import uk.org.hecaton.config.MultiMapConfig;
import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.locksupport.LockStoreContainer;
import uk.org.hecaton.internal.locksupport.LockStoreImpl;
import uk.org.hecaton.internal.locksupport.LockSupportService;
import uk.org.hecaton.internal.locksupport.LockSupportServiceImpl;
import uk.org.hecaton.jet.impl.JobRepository;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static uk.org.hecaton.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MultiMapLockTest extends HecatonTestSupport {

    @Test(expected = NullPointerException.class)
    public void testLock_whenNullKey() {
        MultiMap<String, Integer> multiMap = getMultiMapForLock();
        multiMap.lock(null);
    }

    @Test(expected = NullPointerException.class)
    public void testUnlock_whenNullKey() {
        MultiMap<String, Integer> multiMap = getMultiMapForLock();
        multiMap.unlock(null);
    }

    @Test(timeout = 60000)
    public void testTryLockLeaseTime_whenLockFree() throws InterruptedException {
        MultiMap<String, Integer> multiMap = getMultiMapForLock();
        String key = randomString();
        boolean isLocked = multiMap.tryLock(key, 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
        assertTrue(isLocked);
    }

    @Test(timeout = 60000)
    public void testTryLockLeaseTime_whenLockAcquiredByOther() throws InterruptedException {
        final MultiMap<String, Integer> multiMap = getMultiMapForLock();
        final String key = randomString();
        Thread thread = new Thread(() -> multiMap.lock(key));
        thread.start();
        thread.join();

        boolean isLocked = multiMap.tryLock(key, 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
        Assert.assertFalse(isLocked);
    }

    @Test
    public void testTryLockLeaseTime_lockIsReleasedEventually() throws InterruptedException {
        final MultiMap<String, Integer> multiMap = getMultiMapForLock();
        final String key = randomString();
        multiMap.tryLock(key, 1000, TimeUnit.MILLISECONDS, 1000, TimeUnit.MILLISECONDS);
        assertTrueEventually(() -> Assert.assertFalse(multiMap.isLocked(key)), 30);
    }

    @Test
    public void testLock() throws Exception {
        final String name = "defMM";

        Config config = new Config();
        config.getMultiMapConfig(name)
                .setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);

        TestHecatonInstanceFactory factory = createHecatonInstanceFactory(4);
        final HecatonInstance[] instances = factory.newInstances(config);

        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        new Thread(() -> {
            instances[0].getMultiMap(name).lock("alo");
            latch.countDown();
            try {
                latch2.await(10, TimeUnit.SECONDS);
                instances[0].getMultiMap(name).unlock("alo");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertFalse(instances[0].getMultiMap(name).tryLock("alo"));
        latch2.countDown();
        assertTrue(instances[0].getMultiMap(name).tryLock("alo", 20, TimeUnit.SECONDS));

        new Thread(() -> {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            instances[0].shutdown();
        }).start();

        assertTrue(instances[1].getMultiMap(name).tryLock("alo", 20, TimeUnit.SECONDS));
    }

    /**
     * See issue #4888
     */
    @Test
    public void lockStoreShouldBeRemoved_whenMultimapIsDestroyed() {
        HecatonInstance hz = createHecatonInstance();
        MultiMap<String, Integer> multiMap = hz.getMultiMap(randomName());
        for (int i = 0; i < 1000; i++) {
            multiMap.lock("" + i);
        }
        multiMap.destroy();

        NodeEngineImpl nodeEngine = getNodeEngineImpl(hz);
        LockSupportServiceImpl lockService = nodeEngine.getService(LockSupportService.SERVICE_NAME);
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            LockStoreContainer lockContainer = lockService.getLockContainer(i);
            Collection<LockStoreImpl> lockStores = lockContainer.getLockStores()
                    .stream()
                    .filter(s -> !s.getNamespace().getObjectName().startsWith(JobRepository.INTERNAL_JET_OBJECTS_PREFIX))
                    .collect(Collectors.toList());
            assertEquals("LockStores should be empty: " + lockStores, 0, lockStores.size());
        }
    }

    private MultiMap<String, Integer> getMultiMapForLock() {
        return createHecatonInstance().getMultiMap(randomString());
    }
}
