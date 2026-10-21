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

package uk.org.hecaton.cp.internal.datastructures.semaphore;

import uk.org.hecaton.cp.CPGroupId;
import uk.org.hecaton.cp.ISemaphore;
import uk.org.hecaton.cp.internal.RaftOp;
import uk.org.hecaton.cp.internal.datastructures.AbstractAtomicRegisterSnapshotTest;
import uk.org.hecaton.cp.internal.datastructures.semaphore.operation.AvailablePermitsOp;
import uk.org.hecaton.cp.internal.datastructures.semaphore.proxy.SessionAwareSemaphoreProxy;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SemaphoreSnapshotTest extends AbstractAtomicRegisterSnapshotTest<Integer> {

    private ISemaphore semaphore;
    private String name = "semaphore";


    @Before
    public void createProxy() {
        semaphore = getCPSubsystem().getSemaphore(name);
    }

    @Override
    protected CPGroupId getGroupId() {
        return ((SessionAwareSemaphoreProxy) semaphore).getGroupId();
    }

    @Override
    protected Integer setAndGetInitialValue() {
        assertTrue(semaphore.init(5));
        assertTrue(semaphore.tryAcquire());
        return semaphore.availablePermits();
    }

    @Override
    protected RaftOp getQueryRaftOp() {
        return new AvailablePermitsOp(name);
    }
}
