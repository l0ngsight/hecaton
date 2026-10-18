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

package uk.org.hecaton.internal.metrics.managementcenter;

import uk.org.hecaton.internal.metrics.impl.MetricsService;
import uk.org.hecaton.internal.metrics.managementcenter.ConcurrentArrayRingbuffer.RingbufferSlice;
import uk.org.hecaton.logging.ILogger;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.ReadonlyOperation;
import uk.org.hecaton.spi.impl.operationservice.SelfResponseOperation;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import static uk.org.hecaton.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static uk.org.hecaton.internal.util.ExceptionUtil.peel;
import static uk.org.hecaton.internal.util.ExceptionUtil.withTryCatch;

public class ReadMetricsOperation extends Operation implements ReadonlyOperation, IdentifiedDataSerializable,
                                                               SelfResponseOperation {

    private long offset;

    public ReadMetricsOperation() {
    }

    public ReadMetricsOperation(long offset) {
        this.offset = offset;
    }

    @Override
    public void beforeRun() {
        MetricsService service = getService();
        service.getLiveOperationRegistry().register(this);
    }

    @Override
    public void run() {
        ILogger logger = getNodeEngine().getLogger(getClass());
        MetricsService service = getService();
        CompletableFuture<RingbufferSlice<Entry<Long, byte[]>>> future = service.readMetrics(offset);
        future.whenCompleteAsync(
                withTryCatch(logger, (slice, error) -> doSendResponse(error != null ? peel(error) : slice))
                , CALLER_RUNS);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public Object getResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServiceName() {
        return MetricsService.SERVICE_NAME;
    }

    private void doSendResponse(Object value) {
        try {
            sendResponse(value);
        } finally {
            final MetricsService service = getService();
            service.getLiveOperationRegistry().deregister(this);
        }
    }

    @Override
    public int getFactoryId() {
        return MetricsDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return MetricsDataSerializerHook.READ_METRICS;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(offset);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        offset = in.readLong();
    }
}
