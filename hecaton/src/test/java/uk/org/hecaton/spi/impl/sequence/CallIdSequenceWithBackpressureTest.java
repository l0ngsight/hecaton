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

package uk.org.hecaton.spi.impl.sequence;

import uk.org.hecaton.core.HecatonOverloadException;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.internal.util.ConcurrencyDetection;
import uk.org.hecaton.spi.impl.operationservice.impl.DummyBackupAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.impl.DummyOperation;
import uk.org.hecaton.spi.impl.operationservice.impl.DummyPriorityOperation;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.HecatonTestSupport;
import uk.org.hecaton.test.RequireAssertEnabled;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static uk.org.hecaton.spi.impl.operationservice.OperationAccessor.setCallId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CallIdSequenceWithBackpressureTest extends HecatonTestSupport {

    CallIdSequenceWithBackpressure sequence = new CallIdSequenceWithBackpressure(100, 60000, ConcurrencyDetection.createDisabled());

    @Test
    public void test() {
        assertEquals(0, sequence.getLastCallId());
        assertEquals(100, sequence.getMaxConcurrentInvocations());
    }

    @Test
    public void whenNext_thenSequenceIncrements() {
        // regular operation
        testNext(new DummyOperation());

        // backup-aware operation
        testNext(new DummyBackupAwareOperation());

        // urgent operation
        testNext(new DummyPriorityOperation());
    }

    private void testNext(Operation operation) {
        long oldSequence = sequence.getLastCallId();
        long result = nextCallId(sequence, operation.isUrgent());
        assertEquals(oldSequence + 1, result);
        assertEquals(oldSequence + 1, sequence.getLastCallId());
    }

    @Test
    public void next_whenNoCapacity_thenBlockTillCapacity() throws InterruptedException {
        sequence = new CallIdSequenceWithBackpressure(1, 60000, ConcurrencyDetection.createDisabled());
        final long oldLastCallId = sequence.getLastCallId();

        final CountDownLatch nextCalledLatch = new CountDownLatch(1);
        spawn((Runnable) () -> {
            DummyBackupAwareOperation op = new DummyBackupAwareOperation();
            long callId = nextCallId(sequence, op.isUrgent());
            setCallId(op, callId);
            nextCalledLatch.countDown();
            sleepSeconds(3);
            sequence.complete();
        });

        nextCalledLatch.await();

        long result = nextCallId(sequence, false);
        assertEquals(oldLastCallId + 2, result);
        assertEquals(oldLastCallId + 2, sequence.getLastCallId());
    }

    @Test
    public void next_whenNoCapacity_thenBlockTillTimeout() {
        sequence = new CallIdSequenceWithBackpressure(1, 2000, ConcurrencyDetection.createDisabled());

        // first invocation consumes the available call ID
        nextCallId(sequence, false);

        long oldLastCallId = sequence.getLastCallId();
        try {
            sequence.next();
            fail();
        } catch (HecatonOverloadException e) {
            // expected
        }

        assertEquals(oldLastCallId, sequence.getLastCallId());
    }

    @Test
    public void when_overCapacityButPriorityItem_then_noBackpressure() {
        CallIdSequenceWithBackpressure sequence = new CallIdSequenceWithBackpressure(1, 60000, ConcurrencyDetection.createDisabled());

        // occupy the single call ID slot
        nextCallId(sequence, true);

        long oldLastCallId = sequence.getLastCallId();

        long result = nextCallId(sequence, true);
        assertEquals(oldLastCallId + 1, result);
        assertEquals(oldLastCallId + 1, sequence.getLastCallId());
    }

    @Test
    public void whenComplete_thenTailIncrements() {
        nextCallId(sequence, false);

        long oldSequence = sequence.getLastCallId();
        long oldTail = sequence.getTail();
        sequence.complete();

        assertEquals(oldSequence, sequence.getLastCallId());
        assertEquals(oldTail + 1, sequence.getTail());
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void complete_whenNoMatchingNext() {
        nextCallId(sequence, false);
        sequence.complete();
        sequence.complete();
    }

    static long nextCallId(CallIdSequence seq, boolean isUrgent) {
        return isUrgent ? seq.forceNext() : seq.next();
    }
}
