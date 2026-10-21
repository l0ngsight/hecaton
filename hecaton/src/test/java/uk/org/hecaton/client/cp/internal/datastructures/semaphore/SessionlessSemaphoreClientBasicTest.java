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
import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.ISemaphore;
import uk.org.hecaton.cp.internal.datastructures.semaphore.SessionlessSemaphoreBasicTest;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.TestHecatonInstanceFactory;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;

@RunWith(HecatonParallelClassRunner.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class SessionlessSemaphoreClientBasicTest extends SessionlessSemaphoreBasicTest {

    private HecatonInstance client;

    @Override
    protected TestHecatonInstanceFactory createTestFactory() {
        return new TestHecatonFactory();
    }

    @Override
    protected HecatonInstance[] createInstances() {
        HecatonInstance[] instances = super.createInstances();
        TestHecatonFactory f = (TestHecatonFactory) factory;
        client = f.newHecatonClient();
        return instances;
    }

    @Override
    protected ISemaphore createSemaphore() {
        return client.getCPSubsystem().getSemaphore(objectName + "@group");
    }

    @Override
    protected CPGroupId getGroupId(ISemaphore semaphore) {
        return ((SessionlessSemaphoreProxy) semaphore).getGroupId();
    }

    @Test
    public void testAcquireOnMultipleProxies() {
        ISemaphore semaphore2 = ((TestHecatonFactory) factory).newHecatonClient()
                .getCPSubsystem().getSemaphore(semaphore.getName());

        semaphore.init(1);
        semaphore.tryAcquire(1);

        assertFalse(semaphore2.tryAcquire());
    }

}
