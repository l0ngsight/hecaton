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

package uk.org.hecaton.scheduledexecutor.impl.operations;

import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.scheduledexecutor.impl.DistributedScheduledExecutorService;
import uk.org.hecaton.scheduledexecutor.impl.ScheduledExecutorContainer;
import uk.org.hecaton.scheduledexecutor.impl.ScheduledExecutorDataSerializerHook;
import uk.org.hecaton.spi.impl.operationservice.NamedOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;
import java.util.concurrent.RejectedExecutionException;

public abstract class AbstractSchedulerOperation
        extends Operation
        implements NamedOperation, PartitionAwareOperation, IdentifiedDataSerializable {

    protected String schedulerName;

    AbstractSchedulerOperation() {
    }

    AbstractSchedulerOperation(String schedulerName) {
        this.schedulerName = schedulerName;
    }

    public String getSchedulerName() {
        return schedulerName;
    }

    @Override
    public String getName() {
        return schedulerName;
    }

    @Override
    public String getServiceName() {
        return DistributedScheduledExecutorService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ScheduledExecutorDataSerializerHook.F_ID;
    }

    public ScheduledExecutorContainer getContainer() {
        checkNotShutdown();
        DistributedScheduledExecutorService service = getService();
        return service.getPartitionOrMemberBin(getPartitionId()).getOrCreateContainer(schedulerName);
    }

    private void checkNotShutdown() {
        DistributedScheduledExecutorService service = getService();
        if (service.isShutdown(getSchedulerName())) {
            throw new RejectedExecutionException("Executor is shut down.");
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeString(schedulerName);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        this.schedulerName = in.readString();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", name=").append(schedulerName);
        sb.append(", partitionId=").append(getPartitionId());
    }
}

