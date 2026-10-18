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

package uk.org.hecaton.map.impl.querycache.accumulator;

import uk.org.hecaton.map.impl.querycache.QueryCacheContext;
import uk.org.hecaton.map.impl.querycache.event.QueryCacheEventData;
import uk.org.hecaton.map.impl.querycache.event.sequence.DefaultPartitionSequencer;
import uk.org.hecaton.map.impl.querycache.event.sequence.PartitionSequencer;
import uk.org.hecaton.map.impl.querycache.event.sequence.Sequenced;
import uk.org.hecaton.internal.util.Clock;

/**
 * Contains helpers for an {@code Accumulator} implementation.
 *
 * @param <E> the type of element which will be accumulated.
 */
abstract class AbstractAccumulator<E extends Sequenced> implements Accumulator<E> {

    protected final AccumulatorInfo info;
    protected final QueryCacheContext context;
    protected final CyclicBuffer<E> buffer;
    protected final PartitionSequencer partitionSequencer;

    AbstractAccumulator(QueryCacheContext context, AccumulatorInfo info) {
        this.context = context;
        this.info = info;
        this.partitionSequencer = new DefaultPartitionSequencer();
        this.buffer = new DefaultCyclicBuffer<>(info.getBufferSize());
    }

    public CyclicBuffer<E> getBuffer() {
        return buffer;
    }

    protected QueryCacheContext getContext() {
        return context;
    }

    protected long getNow() {
        return Clock.currentTimeMillis();
    }

    protected boolean isExpired(QueryCacheEventData entry, long delayMillis, long now) {
        return entry != null
                && (now - entry.getCreationTime()) >= delayMillis;
    }

    @Override
    public void reset() {
        buffer.reset();
        partitionSequencer.reset();
    }
}
