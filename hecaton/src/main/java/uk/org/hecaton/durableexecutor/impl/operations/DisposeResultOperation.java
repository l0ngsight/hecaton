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

package uk.org.hecaton.durableexecutor.impl.operations;

import uk.org.hecaton.durableexecutor.impl.DurableExecutorContainer;
import uk.org.hecaton.durableexecutor.impl.DurableExecutorDataSerializerHook;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.spi.impl.operationservice.BackupAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;

public class DisposeResultOperation extends AbstractDurableExecutorOperation implements BackupAwareOperation,
        MutatingOperation {

    int sequence;

    public DisposeResultOperation() {
    }

    public DisposeResultOperation(String name, int sequence) {
        super(name);
        this.sequence = sequence;
    }

    @Override
    public void run() throws Exception {
        DurableExecutorContainer executorContainer = getExecutorContainer();
        executorContainer.disposeResult(sequence);
    }

    @Override
    public Operation getBackupOperation() {
        return new DisposeResultBackupOperation(name, sequence);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(sequence);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        sequence = in.readInt();
    }

    @Override
    public int getClassId() {
        return DurableExecutorDataSerializerHook.DISPOSE_RESULT;
    }
}
