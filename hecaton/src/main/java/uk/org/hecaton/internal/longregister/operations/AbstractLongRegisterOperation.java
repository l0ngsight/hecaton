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

package uk.org.hecaton.internal.longregister.operations;

import uk.org.hecaton.internal.longregister.LongRegister;
import uk.org.hecaton.internal.longregister.LongRegisterDataSerializerHook;
import uk.org.hecaton.internal.longregister.LongRegisterService;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.NamedOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.PartitionAwareOperation;

import java.io.IOException;

public abstract class AbstractLongRegisterOperation extends Operation
        implements NamedOperation, PartitionAwareOperation, IdentifiedDataSerializable {

    protected String name;

    public AbstractLongRegisterOperation() {
    }

    public AbstractLongRegisterOperation(String name) {
        this.name = name;
    }

    public LongRegister getLongContainer() {
        LongRegisterService service = getService();
        return service.getLongRegister(name);
    }

    @Override
    public final String getServiceName() {
        return LongRegisterService.SERVICE_NAME;
    }

    @Override
    public final String getName() {
        return name;
    }

    @Override
    public final int getFactoryId() {
        return LongRegisterDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readString();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", name=").append(name);
    }
}
