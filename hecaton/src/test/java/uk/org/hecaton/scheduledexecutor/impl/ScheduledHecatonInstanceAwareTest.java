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

package uk.org.hecaton.scheduledexecutor.impl;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.scheduledexecutor.IScheduledExecutorService;
import uk.org.hecaton.scheduledexecutor.IScheduledFuture;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ScheduledHecatonInstanceAwareTest extends ScheduledExecutorServiceTestSupport {

    private HecatonInstance[] members;
    private IScheduledExecutorService scheduledExecutorService;

    @Before
    public void setup() {
        members = createClusterWithCount(2);
        scheduledExecutorService = getScheduledExecutor(members, randomName());
    }

    @Test
    public void test_hecatonInstanceIsInjected_whenSchedulingOnSameMember() throws Exception {
        IScheduledFuture<Boolean> injected = scheduledExecutorService.schedule(
                new HecatonInstanceAwareRunnable(randomNameOwnedBy(members[0])), 200, MILLISECONDS);
        assertTrue("HecatonInstance should have been injected", injected.get());
    }

    @Test
    public void test_hecatonInstanceIsInjected_whenSchedulingOnOtherMember() throws Exception {
        IScheduledFuture<Boolean> injected = scheduledExecutorService.schedule(
                new HecatonInstanceAwareRunnable(randomNameOwnedBy(members[1])), 200, MILLISECONDS);
        assertTrue("HecatonInstance should have been injected", injected.get());
    }
}
