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
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;

import static uk.org.hecaton.internal.longregister.LongRegisterDataSerializerHook.GET_AND_ADD;

public class GetAndAddOperation extends LongRegisterBackupAwareOperation implements MutatingOperation {

    private long delta;
    private long returnValue;

    public GetAndAddOperation() {
    }

    public GetAndAddOperation(String name, long delta) {
        super(name);
        this.delta = delta;
    }

    @Override
    public void run() throws Exception {
        LongRegister container = getLongContainer();
        returnValue = container.getAndAdd(delta);
    }

    @Override
    public Object getResponse() {
        return returnValue;
    }

    @Override
    public Operation getBackupOperation() {
        return new AddBackupOperation(name, delta);
    }

    @Override
    public int getClassId() {
        return GET_AND_ADD;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(delta);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        delta = in.readLong();
    }
}
