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

package uk.org.hecaton.jet.impl.processor;

import uk.org.hecaton.function.ToLongFunctionEx;
import uk.org.hecaton.jet.accumulator.LongAccumulator;
import uk.org.hecaton.jet.aggregate.AggregateOperation1;
import uk.org.hecaton.jet.config.ProcessingGuarantee;
import uk.org.hecaton.jet.core.BroadcastKey;
import uk.org.hecaton.jet.core.Outbox;
import uk.org.hecaton.jet.core.Processor.Context;
import uk.org.hecaton.jet.core.SlidingWindowPolicy;
import uk.org.hecaton.jet.core.test.TestOutbox;
import uk.org.hecaton.jet.core.test.TestProcessorContext;
import uk.org.hecaton.jet.datamodel.KeyedWindowResult;
import uk.org.hecaton.jet.impl.processor.SlidingWindowP.Keys;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map.Entry;

import static uk.org.hecaton.function.Functions.entryKey;
import static uk.org.hecaton.jet.aggregate.AggregateOperations.counting;
import static uk.org.hecaton.jet.config.ProcessingGuarantee.AT_LEAST_ONCE;
import static uk.org.hecaton.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(HecatonParallelClassRunner.class)
@Category(ParallelJVMTest.class)
public class SlidingWindowP_failoverTest {

    private SlidingWindowP<Entry<String, Long>, ?, Long, ?> p;

    private void init(ProcessingGuarantee guarantee) throws Exception {
        SlidingWindowPolicy wDef = SlidingWindowPolicy.tumblingWinPolicy(1);
        AggregateOperation1<Object, LongAccumulator, Long> aggrOp = counting();
        p = new SlidingWindowP<>(
                singletonList(entryKey()),
                singletonList((ToLongFunctionEx<Entry<?, Long>>) Entry::getValue),
                wDef,
                0L,
                aggrOp,
                KeyedWindowResult::new,
                true,
                (byte) 0);

        Outbox outbox = new TestOutbox(128);
        Context context = new TestProcessorContext()
                .setProcessingGuarantee(guarantee);
        p.init(outbox, context);
    }

    @Test
    public void when_differentWmExactlyOnce_then_fail() throws Exception {
        init(EXACTLY_ONCE);

        BroadcastKey<Keys> broadcastKey = BroadcastKey.broadcastKey(Keys.NEXT_WIN_TO_EMIT);
        p.restoreFromSnapshot(broadcastKey, 1L);
        assertThrows(AssertionError.class, () -> p.restoreFromSnapshot(broadcastKey, 2L));
    }

    @Test
    public void when_differentWmAtLeastOnce_then_useMin() throws Exception {
        init(AT_LEAST_ONCE);

        p.restoreFromSnapshot(BroadcastKey.broadcastKey(Keys.NEXT_WIN_TO_EMIT), 2L);
        p.restoreFromSnapshot(BroadcastKey.broadcastKey(Keys.NEXT_WIN_TO_EMIT), 1L);
        p.finishSnapshotRestore();

        assertEquals(1L, p.nextWinToEmit);
    }

    @Test
    public void when_noSnapshotRestored_then_wmIsMin() throws Exception {
        init(AT_LEAST_ONCE);

        assertEquals(Long.MIN_VALUE, p.nextWinToEmit);
    }
}
