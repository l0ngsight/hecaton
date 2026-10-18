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

package uk.org.hecaton.spi.impl.operationparker.impl;

import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.AbstractLocalOperation;
import uk.org.hecaton.spi.impl.operationservice.BlockingOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.OperationResponseHandler;
import uk.org.hecaton.spi.impl.operationservice.OperationService;
import uk.org.hecaton.spi.impl.operationservice.PartitionAwareOperation;
import uk.org.hecaton.spi.exception.RetryableException;
import uk.org.hecaton.spi.impl.NodeEngineImpl;
import uk.org.hecaton.spi.impl.operationservice.impl.OperationServiceImpl;
import uk.org.hecaton.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import uk.org.hecaton.internal.util.Clock;

import java.util.Queue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import static uk.org.hecaton.internal.util.EmptyStatement.ignore;

/**
 * A simple container for a {@link BlockingOperation} that is added to the {@link WaitSet}.
 *
 * Each WaitSetEntry is put in a delay queue (part of the {@link OperationParkerImpl}) based on its expiration time.
 */
class WaitSetEntry extends AbstractLocalOperation implements Delayed, PartitionAwareOperation, IdentifiedDataSerializable {

    final Queue<WaitSetEntry> queue;
    final Operation op;
    final BlockingOperation blockingOperation;
    final long expirationTimeMs;
    volatile boolean valid = true;
    volatile Object cancelResponse;

    WaitSetEntry(Queue<WaitSetEntry> queue, BlockingOperation blockingOperation) {
        this.op = (Operation) blockingOperation;
        this.blockingOperation = blockingOperation;
        this.queue = queue;
        this.expirationTimeMs = getExpirationTimeMs(blockingOperation);

        setPartitionId(op.getPartitionId());
    }

    private long getExpirationTimeMs(BlockingOperation blockingOperation) {
        long waitTimeout = blockingOperation.getWaitTimeout();
        if (waitTimeout < 0) {
            return -1;
        }
        long expirationTime = Clock.currentTimeMillis() + waitTimeout;
        if (expirationTime < 0) {
            return -1;
        }
        return expirationTime;
    }

    public Operation getOperation() {
        return op;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public boolean isValid() {
        return valid;
    }

    public boolean needsInvalidation() {
        return isExpired() || isCancelled() || isCallTimedOut();
    }

    public boolean isExpired() {
        return expirationTimeMs > 0 && Clock.currentTimeMillis() >= expirationTimeMs;
    }

    public boolean isCancelled() {
        return cancelResponse != null;
    }

    public boolean isCallTimedOut() {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        OperationServiceImpl operationService = nodeEngine.getOperationService();
        if (operationService.isCallTimedOut(op)) {
            cancel(new CallTimeoutResponse(op.getCallId(), op.isUrgent()));
            return true;
        }
        return false;
    }

    public boolean shouldWait() {
        return blockingOperation.shouldWait();
    }

    public void unpark(OperationService operationService) {
        blockingOperation.unpark(operationService);
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(expirationTimeMs - Clock.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        // compare zero ONLY if same object
        if (other == this) {
            return 0;
        }
        long d = (getDelay(TimeUnit.NANOSECONDS)
                - other.getDelay(TimeUnit.NANOSECONDS));
        return (d == 0) ? 0 : ((d < 0) ? -1 : 1);
    }

    @Override
    public void run() throws Exception {
        if (!valid) {
            return;
        }

        boolean expired = isExpired();
        boolean cancelled = isCancelled();
        if (!expired && !cancelled) {
            return;
        }

        if (!queue.remove(this)) {
            return;
        }

        valid = false;
        if (expired) {
            onExpire();
        } else {
            onCancel();
        }
    }

    // if you don't think instances of this class will ever be inserted into a HashMap/HashTable,
    // the recommended hashCode implementation to use is:
    @Override
    public int hashCode() {
        assert false : "hashCode not designed";
        return 42;
        // any arbitrary constant will do
    }

    @Override
    // use object.equals
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public void logError(Throwable e) {
        ILogger logger = getLogger();
        if (e instanceof RetryableException) {
            logger.warning("Op: " + op + ", " + e.getClass().getName() + ": " + e.getMessage());
        } else if (e instanceof OutOfMemoryError) {
            try {
                logger.severe(e.getMessage(), e);
            } catch (Throwable ignored) {
                ignore(ignored);
            }
        } else {
            logger.severe("Op: " + op + ", Error: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public String getServiceName() {
        return op.getServiceName();
    }

    public void onExpire() {
        blockingOperation.onWaitExpire();
    }

    public void onCancel() {
        OperationResponseHandler responseHandler = op.getOperationResponseHandler();
        responseHandler.sendResponse(op, cancelResponse);
    }

    public void cancel(Object error) {
        this.cancelResponse = error;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", op=").append(op);
        sb.append(", expirationTimeMs=").append(expirationTimeMs);
        sb.append(", valid=").append(valid);
    }
}
