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

package uk.org.hecaton.internal.util.concurrent;

import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static uk.org.hecaton.internal.util.concurrent.ConcurrentConveyorSingleQueue.concurrentConveyorSingleQueue;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConcurrentConveyorSingleQueueTest extends ConcurrentConveyorTest {

    private ConcurrentConveyorSingleQueue<Item> conveyorSingleQueue;

    @Before
    @Override
    public void before() {
        queueCount = 1;
        defaultQ = new OneToOneConcurrentArrayQueue<>(QUEUE_CAPACITY);
        conveyorSingleQueue = concurrentConveyorSingleQueue(doneItem, defaultQ);
        conveyor = conveyorSingleQueue;
    }

    @Test
    @Override
    public void when_drainQueue1ToList_then_listPopulated() {
        // FIXME
    }

    @Test
    @Override
    public void when_drainQueue1ToListLimited_then_listHasLimitedItems() {
        // FIXME
    }

    @Test
    @Override
    public void when_drainerFails_then_offerFailsWithItsFailureAsCause() {
        // FIXME
    }

    @Test
    @Override
    public void when_drainerDone_then_offerToFullQueueFails() {
        // FIXME
    }

    @Test
    public void when_offer_then_poll() {
        // when
        boolean didOffer = conveyorSingleQueue.offer(item1);

        // then
        assertTrue(didOffer);
        assertSame(item1, defaultQ.poll());
    }

    @Test
    public void when_submit_then_poll() {
        // when
        conveyorSingleQueue.submit(item2);

        // then
        assertSame(item2, defaultQ.poll());
    }
}
