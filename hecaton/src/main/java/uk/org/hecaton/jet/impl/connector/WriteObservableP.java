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

package uk.org.hecaton.jet.impl.connector;

import uk.org.hecaton.core.HecatonInstance;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.jet.core.Inbox;
import uk.org.hecaton.jet.core.Outbox;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.impl.observer.ObservableImpl;
import uk.org.hecaton.ringbuffer.OverflowPolicy;
import uk.org.hecaton.ringbuffer.Ringbuffer;
import uk.org.hecaton.ringbuffer.impl.RingbufferProxy;

import javax.annotation.Nonnull;
import java.io.Serial;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public final class WriteObservableP<T> extends AsyncHecatonWriterP {

    private static final int MAX_PARALLEL_ASYNC_OPS = 1;
    private static final int MAX_BATCH_SIZE = RingbufferProxy.MAX_BATCH_SIZE;

    private final String observableName;
    private final Function<T, Data> mapper;
    private final List<Data> batch = new ArrayList<>(MAX_BATCH_SIZE);

    private Ringbuffer<Data> ringbuffer;

    private WriteObservableP(String observableName,
                             @Nonnull HecatonInstance instance,
                             @Nonnull SerializationService serializationService) {
        super(instance, MAX_PARALLEL_ASYNC_OPS);
        this.observableName = observableName;
        this.mapper = serializationService::toData;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        // we want to potentially create the Ringbuffer as lately as possible to
        // maximize the window when its properties (like capacity) can still be
        // configured
        ringbuffer = instance().getRingbuffer(ObservableImpl.ringbufferName(observableName));
    }

    @Override
    protected void processInternal(Inbox inbox) {
        if (batch.size() < MAX_BATCH_SIZE) {
            inbox.drainTo(batch, MAX_BATCH_SIZE - batch.size(), mapper);
        }
        tryFlush();
    }

    @Override
    protected boolean flushInternal() {
        return tryFlush();
    }

    private boolean tryFlush() {
        if (batch.isEmpty()) {
            return true;
        }
        if (!tryAcquirePermit()) {
            return false;
        }
        setCallback(ringbuffer.addAllAsync(batch, OverflowPolicy.OVERWRITE));
        batch.clear();
        return true;
    }

    public static final class Supplier extends AbstractHecatonConnectorSupplier {

        @Serial
        private static final long serialVersionUID = 1L;

        private final String observableName;

        public Supplier(String observableName) {
            super(null, null);
            this.observableName = observableName;
        }

        @Override
        protected Processor createProcessor(HecatonInstance instance, SerializationService serializationService) {
            return new WriteObservableP<>(observableName, instance, serializationService);
        }
    }
}
