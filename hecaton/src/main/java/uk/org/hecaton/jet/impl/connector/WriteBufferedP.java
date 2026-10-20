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

import uk.org.hecaton.core.ManagedContext;
import uk.org.hecaton.function.BiConsumerEx;
import uk.org.hecaton.function.ConsumerEx;
import uk.org.hecaton.function.FunctionEx;
import uk.org.hecaton.function.SupplierEx;
import uk.org.hecaton.jet.JetException;
import uk.org.hecaton.jet.core.Inbox;
import uk.org.hecaton.jet.core.Outbox;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.Watermark;
import uk.org.hecaton.jet.core.processor.SinkProcessors;
import uk.org.hecaton.security.PermissionsUtil;

import javax.annotation.Nonnull;
import java.util.function.Consumer;

import static uk.org.hecaton.jet.impl.util.Util.checkSerializable;

public final class WriteBufferedP<B, T> implements Processor {

    private final FunctionEx<? super Context, B> createFn;
    private final ConsumerEx<? super B> flushFn;
    private final ConsumerEx<? super B> destroyFn;

    private B buffer;
    private final Consumer<Object> inboxConsumer;

    WriteBufferedP(
            @Nonnull FunctionEx<? super Context, B> createFn,
            @Nonnull BiConsumerEx<? super B, ? super T> onReceiveFn,
            @Nonnull ConsumerEx<? super B> flushFn,
            @Nonnull ConsumerEx<? super B> destroyFn
    ) {
        this.createFn = createFn;
        this.flushFn = flushFn;
        this.destroyFn = destroyFn;

        inboxConsumer = item -> onReceiveFn.accept(buffer, (T) item);
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        PermissionsUtil.checkPermission(createFn, context);
        B localBuff = createFn.apply(context);
        if (localBuff == null) {
            throw new JetException("Null buffer created");
        }
        ManagedContext managedContext = context.managedContext();
        buffer = (B) managedContext.initialize(localBuff);
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        inbox.drain(inboxConsumer);
        flushFn.accept(buffer);
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        // we're a sink, no need to forward the watermarks
        return true;
    }

    @Override
    public void close() {
        if (buffer != null) {
            destroyFn.accept(buffer);
        }
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    /**
     * This is private API. Call {@link SinkProcessors#writeBufferedP} instead.
     */
    @Nonnull
    public static <B, T> SupplierEx<Processor> supplier(
            @Nonnull FunctionEx<? super Context, ? extends B> createFn,
            @Nonnull BiConsumerEx<? super B, ? super T> onReceiveFn,
            @Nonnull ConsumerEx<? super B> flushFn,
            @Nonnull ConsumerEx<? super B> destroyFn
    ) {
        checkSerializable(createFn, "createFn");
        checkSerializable(onReceiveFn, "onReceiveFn");
        checkSerializable(flushFn, "flushFn");
        checkSerializable(destroyFn, "destroyFn");

        return () -> new WriteBufferedP<>(createFn, onReceiveFn, flushFn, destroyFn);
    }
}
