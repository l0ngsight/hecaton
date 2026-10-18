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

package uk.org.hecaton.jet.impl.execution;

import uk.org.hecaton.internal.serialization.impl.HeapData;
import uk.org.hecaton.jet.config.ProcessingGuarantee;
import uk.org.hecaton.jet.core.JetTestSupport;
import uk.org.hecaton.jet.impl.operation.SnapshotPhase1Operation.SnapshotPhase1Result;
import uk.org.hecaton.jet.impl.util.MockAsyncSnapshotWriter;
import uk.org.hecaton.logging.Logger;
import uk.org.hecaton.test.HecatonParallelClassRunner;
import uk.org.hecaton.test.annotation.ParallelJVMTest;
import uk.org.hecaton.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.jet.Util.entry;
import static uk.org.hecaton.jet.impl.execution.DoneItem.DONE_ITEM;
import static uk.org.hecaton.jet.impl.util.ProgressState.DONE;
import static uk.org.hecaton.jet.impl.util.ProgressState.MADE_PROGRESS;
import static uk.org.hecaton.jet.impl.util.ProgressState.NO_PROGRESS;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HecatonParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StoreSnapshotTaskletTest extends JetTestSupport {

    private SnapshotContext ssContext;
    private MockInboundStream input;
    private StoreSnapshotTasklet sst;
    private MockAsyncSnapshotWriter mockSsWriter;

    private void init(List<Object> inputData) {
        ssContext = new SnapshotContext(Logger.getLogger(SnapshotContext.class), "test job", 1,
                ProcessingGuarantee.EXACTLY_ONCE);
        ssContext.initTaskletCount(1, 1, 0);
        inputData = new ArrayList<>(inputData);
        // serialize input data
        for (int i = 0; i < inputData.size(); i++) {
            if (inputData.get(i) instanceof Entry) {
                Entry<String, String> en = (Entry<String, String>) inputData.get(i);
                inputData.set(i, entry(serialize(en.getKey()), serialize(en.getValue())));
            }
        }
        input = new MockInboundStream(0, inputData, 128);
        mockSsWriter = new MockAsyncSnapshotWriter();
        sst = new StoreSnapshotTasklet(ssContext, input, mockSsWriter, Logger.getLogger(mockSsWriter.getClass()),
                "myVertex", false);
    }

    @Test
    public void when_doneItemOnInput_then_eventuallyDone() {
        // When
        init(singletonList(DONE_ITEM));

        // Then
        assertEquals(DONE, sst.call());
    }

    @Test
    public void when_item_then_offeredToSsWriter() {
        // When
        init(singletonList(entry("k", "v")));
        assertEquals(MADE_PROGRESS, sst.call());

        // Then
        assertEquals(entry(serialize("k"), serialize("v")), mockSsWriter.poll());
        assertNull(mockSsWriter.poll());
    }

    @Test
    public void when_notAbleToOffer_then_offeredLater() {
        // When
        init(singletonList(entry("k", "v")));
        mockSsWriter.ableToOffer = false;
        assertEquals(MADE_PROGRESS, sst.call());
        assertEquals(0, input.remainingItems().size());
        assertEquals(NO_PROGRESS, sst.call());
        assertNull(mockSsWriter.poll());

        // Then
        mockSsWriter.ableToOffer = true;
        assertEquals(MADE_PROGRESS, sst.call());
        assertEquals(entry(serialize("k"), serialize("v")), mockSsWriter.poll());
        assertNull(mockSsWriter.poll());
    }

    @Test
    public void when_barrier_then_snapshotDone() {
        // When
        init(singletonList(new SnapshotBarrier(2, false)));
        ssContext.startNewSnapshotPhase1(2, "map", 0);
        assertEquals(MADE_PROGRESS, sst.call());
        assertEquals(MADE_PROGRESS, sst.call());

        // Then
        assertEquals(3, sst.pendingSnapshotId);
    }

    @Test
    public void when_itemAndBarrier_then_snapshotDone() {
        // When
        Entry<String, String> entry = entry("k", "v");
        init(asList(entry, new SnapshotBarrier(2, false)));
        ssContext.startNewSnapshotPhase1(2, "map", 0);
        assertEquals(2, sst.pendingSnapshotId);
        assertEquals(MADE_PROGRESS, sst.call());
        assertEquals(MADE_PROGRESS, sst.call());
        mockSsWriter.hasPendingFlushes = false;
        assertEquals(MADE_PROGRESS, sst.call());

        // Then
        assertEquals(3, sst.pendingSnapshotId);
        assertEquals(entry(serialize("k"), serialize("v")), mockSsWriter.poll());
    }

    @Test
    public void when_notAbleToFlush_then_tryAgain() {
        // When
        init(singletonList(new SnapshotBarrier(2, false)));
        ssContext.startNewSnapshotPhase1(2, "map", 0);
        mockSsWriter.ableToFlushRemaining = false;
        assertEquals(MADE_PROGRESS, sst.call());
        assertEquals(NO_PROGRESS, sst.call());
        assertEquals(NO_PROGRESS, sst.call());

        // Then
        mockSsWriter.ableToFlushRemaining = true;
        assertEquals(MADE_PROGRESS, sst.call());
        assertEquals(MADE_PROGRESS, sst.call());
        assertEquals(NO_PROGRESS, sst.call());
        assertEquals(3, sst.pendingSnapshotId);
    }

    @Test
    public void test_waitingForFlushesToComplete() {
        // When
        Entry<String, String> entry = entry("k", "v");
        init(asList(entry, new SnapshotBarrier(2, false)));
        ssContext.startNewSnapshotPhase1(2, "map", 0);
        assertEquals(MADE_PROGRESS, sst.call());
        assertEquals(MADE_PROGRESS, sst.call());
        assertEquals(NO_PROGRESS, sst.call());
        assertTrue(mockSsWriter.hasPendingFlushes);

        // Then
        mockSsWriter.hasPendingFlushes = false;
        assertEquals(MADE_PROGRESS, sst.call());
        assertEquals(NO_PROGRESS, sst.call());
        assertEquals(3, sst.pendingSnapshotId);
    }

    @Test
    public void when_snapshotFails_then_reportedToContext() throws Exception {
        // When
        init(singletonList(new SnapshotBarrier(2, false)));
        RuntimeException mockFailure = new RuntimeException("mock failure");
        mockSsWriter.failure = mockFailure;
        CompletableFuture<SnapshotPhase1Result> future = ssContext.startNewSnapshotPhase1(2, "map", 0);
        assertEquals(MADE_PROGRESS, sst.call());
        assertFalse(future.isDone());
        assertEquals(MADE_PROGRESS, sst.call());

        // Then
        assertTrue(future.isDone());
        assertEquals(mockFailure.toString(), future.get().getError());
        assertEquals(3, sst.pendingSnapshotId);
    }

    private HeapData serialize(String o) {
        // "abcd" is here to create 8 bytes for HeapData header (we use UTF-16)
        return new HeapData(("abcd" + o).getBytes(StandardCharsets.UTF_16));
    }
}
