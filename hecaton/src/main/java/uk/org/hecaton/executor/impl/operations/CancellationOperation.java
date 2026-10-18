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

package uk.org.hecaton.executor.impl.operations;

import uk.org.hecaton.internal.util.UUIDSerializationUtil;
import uk.org.hecaton.executor.impl.DistributedExecutorService;
import uk.org.hecaton.executor.impl.ExecutorDataSerializerHook;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.NamedOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;
import java.util.UUID;

public final class CancellationOperation extends Operation implements NamedOperation, MutatingOperation,
        IdentifiedDataSerializable {

    private UUID uuid;
    private boolean interrupt;
    private boolean response;

    public CancellationOperation() {
    }

    public CancellationOperation(UUID uuid, boolean interrupt) {
        this.uuid = uuid;
        this.interrupt = interrupt;
    }

    @Override
    public String getServiceName() {
        return DistributedExecutorService.SERVICE_NAME;
    }

    @Override
    public void run() throws Exception {
        DistributedExecutorService service = getService();
        response = service.cancel(uuid, interrupt);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public String getName() {
        DistributedExecutorService service = getService();
        return service.getName(uuid);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        UUIDSerializationUtil.writeUUID(out, uuid);
        out.writeBoolean(interrupt);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        uuid = UUIDSerializationUtil.readUUID(in);
        interrupt = in.readBoolean();
    }

    @Override
    public int getFactoryId() {
        return ExecutorDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ExecutorDataSerializerHook.CANCELLATION;
    }

}
