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

import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.jet.Traverser;
import uk.org.hecaton.jet.core.AbstractProcessor;
import uk.org.hecaton.jet.core.BroadcastKey;
import uk.org.hecaton.jet.core.EventTimeMapper;
import uk.org.hecaton.jet.core.EventTimePolicy;
import uk.org.hecaton.jet.core.ProcessorSupplier;
import uk.org.hecaton.jet.core.processor.Processors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static uk.org.hecaton.jet.core.BroadcastKey.broadcastKey;

/**
 * See {@link Processors#insertWatermarksP}.
 *
 * @param <T> type of the stream item
 */
public class InsertWatermarksP<T> extends AbstractProcessor {

    private final FunctionEx<ProcessorSupplier.Context, EventTimePolicy<? super T>> eventTimePolicyProvider;
    private EventTimeMapper<? super T> eventTimeMapper;
    private Traverser<Object> traverser;

    // value to be used temporarily during snapshot restore
    private long minRestoredWm = Long.MAX_VALUE;

    public InsertWatermarksP(FunctionEx<ProcessorSupplier.Context, EventTimePolicy<? super T>> eventTimePolicyProvider) {
        this.eventTimePolicyProvider = eventTimePolicyProvider;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        eventTimeMapper = new EventTimeMapper<>(eventTimePolicyProvider.apply(context));
        eventTimeMapper.addPartitions(1);
    }

    @Override
    public boolean tryProcess() {
        return tryProcessInternal(null);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        return tryProcessInternal(item);
    }

    @SuppressWarnings("unchecked")
    private boolean tryProcessInternal(@Nullable Object item) {
        if (traverser == null) {
            traverser = eventTimeMapper.flatMapEvent((T) item, 0, EventTimeMapper.NO_NATIVE_TIME);
        }
        if (emitFromTraverser(traverser)) {
            traverser = null;
            return true;
        }
        return false;
    }

    @Override
    public boolean saveToSnapshot() {
        return tryEmitToSnapshot(broadcastKey(Keys.LAST_EMITTED_WM), eventTimeMapper.getWatermark(0));
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        assert ((BroadcastKey) key).key().equals(Keys.LAST_EMITTED_WM) : "Unexpected key: " + key;
        // we restart at the oldest WM any instance was at at the time of snapshot
        minRestoredWm = Math.min(minRestoredWm, (long) value);
    }

    @Override
    public boolean finishSnapshotRestore() {
        eventTimeMapper.restoreWatermark(0, minRestoredWm);
        getLogger().fine("restored lastEmittedWm=%s", minRestoredWm);
        return true;
    }

    @Override
    public boolean closeIsCooperative() {
        return true;
    }

    private enum Keys {
        LAST_EMITTED_WM
    }
}
