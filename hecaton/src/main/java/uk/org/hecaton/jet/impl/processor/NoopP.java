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

import uk.org.hecaton.function.ConsumerEx;
import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.jet.core.Inbox;
import uk.org.hecaton.jet.core.Outbox;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.Watermark;
import uk.org.hecaton.jet.core.processor.Processors;
import uk.org.hecaton.jet.impl.execution.init.JetInitDataSerializerHook;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serial;

/**
 * A no-operation processor. See {@link Processors#noopP()}.
 */
public class NoopP implements Processor {
    private Outbox outbox;

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) throws Exception {
        this.outbox = outbox;
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        inbox.drain(ConsumerEx.noop());
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return outbox.offer(watermark);
    }

    @Override
    public void restoreFromSnapshot(@Nonnull Inbox inbox) {
        inbox.drain(ConsumerEx.noop());
    }

    @Override
    public boolean closeIsCooperative() {
        return true;
    }

    public static final class NoopPSupplier implements SupplierEx<Processor>, IdentifiedDataSerializable {
        @Serial
        private static final long serialVersionUID = 1L;

        @Override
        public Processor getEx() throws Exception {
            return new NoopP();
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetInitDataSerializerHook.NOOP_PROCESSOR_SUPPLIER;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }
}
