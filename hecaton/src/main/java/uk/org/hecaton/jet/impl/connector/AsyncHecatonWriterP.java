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
import uk.org.hecaton.core.HecatonInstanceNotActiveException;
import uk.org.hecaton.jet.core.Inbox;
import uk.org.hecaton.jet.core.Processor;
import uk.org.hecaton.jet.core.Watermark;
import uk.org.hecaton.jet.impl.util.ImdgUtil;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.logging.Logger;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static uk.org.hecaton.internal.util.ExceptionUtil.sneakyThrow;
import static uk.org.hecaton.internal.util.ExceptionUtil.withTryCatch;
import static uk.org.hecaton.jet.impl.connector.HecatonWriters.handleInstanceNotActive;
import static uk.org.hecaton.jet.impl.util.Util.tryIncrement;

public abstract class AsyncHecatonWriterP implements Processor {

    public static final int MAX_PARALLEL_ASYNC_OPS_DEFAULT = 1000;

    private final ILogger logger = Logger.getLogger(AsyncHecatonWriterP.class);
    private final int maxParallelAsyncOps;
    private final AtomicInteger numConcurrentOps = new AtomicInteger();
    private final AtomicReference<Throwable> firstError = new AtomicReference<>();
    private final HecatonInstance instance;
    private final boolean isLocal;

    private final BiConsumer callback = withTryCatch(logger, (response, t) -> {
        numConcurrentOps.decrementAndGet();
        if (t != null) {
            firstError.compareAndSet(null, t);
        }
    });

    protected AsyncHecatonWriterP(@Nonnull HecatonInstance instance, int maxParallelAsyncOps) {
        this.instance = Objects.requireNonNull(instance, "instance");
        this.maxParallelAsyncOps = maxParallelAsyncOps;
        this.isLocal = ImdgUtil.isMemberInstance(instance);
    }

    @Override
    public final boolean tryProcess() {
        flush();
        return true;
    }

    @Override
    public final void process(int ordinal, @Nonnull Inbox inbox) {
        checkError();
        try {
            processInternal(inbox);
        } catch (HecatonInstanceNotActiveException e) {
            throw handleInstanceNotActive(e, isLocal());
        }
    }

    @Override
    public final boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    @Override
    public boolean saveToSnapshot() {
        return flush() && asyncCallsDone();
    }

    @Override
    public final boolean complete() {
        return flush() && asyncCallsDone();
    }

    @Override
    public boolean closeIsCooperative() {
        return true;
    }

    private boolean flush() {
        checkError();
        try {
            return flushInternal();
        } catch (HecatonInstanceNotActiveException e) {
            throw handleInstanceNotActive(e, isLocal());
        }
    }

    @CheckReturnValue
    protected boolean flushInternal() {
        return true;
    }

    protected abstract void processInternal(Inbox inbox);

    protected final void setCallback(CompletionStage stage) {
        stage.whenCompleteAsync(callback);
    }

    @CheckReturnValue
    protected final boolean tryAcquirePermit() {
        return tryIncrement(numConcurrentOps, 1, maxParallelAsyncOps);
    }

    /**
     * Acquires as many permits as we are able to immediately, up to
     * desiredNumber. Returns the number of actually acquired permits. Can
     * return 0.
     */
    @CheckReturnValue
    protected final int tryAcquirePermits(int desiredNumber) {
        int prev;
        int next;
        do {
            prev = numConcurrentOps.get();
            next = Math.min(prev + desiredNumber, maxParallelAsyncOps);
            if (next == prev) {
                return 0;
            }
        } while (!numConcurrentOps.compareAndSet(prev, next));
        return next - prev;
    }

    protected final HecatonInstance instance() {
        return instance;
    }

    protected final boolean isLocal() {
        return isLocal;
    }

    private void checkError() {
        Throwable t = firstError.get();
        if (t instanceof HecatonInstanceNotActiveException hecatonInstanceNotActiveException) {
            throw handleInstanceNotActive(hecatonInstanceNotActiveException, isLocal());
        } else if (t != null) {
            throw sneakyThrow(t);
        }
    }

    private boolean asyncCallsDone() {
        boolean allWritten = numConcurrentOps.get() == 0;
        checkError();
        return allWritten;
    }
}
