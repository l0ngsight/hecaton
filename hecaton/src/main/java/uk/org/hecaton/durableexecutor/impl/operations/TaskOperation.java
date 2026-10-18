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
import uk.org.hecaton.internal.namespace.NamespaceUtil;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.operationservice.BackupAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;
import java.util.concurrent.Callable;

public class TaskOperation extends AbstractDurableExecutorOperation implements BackupAwareOperation, MutatingOperation {

    private Data callableData;
    private transient int sequence;
    private transient Callable callable;

    public TaskOperation() {
    }

    public TaskOperation(String name, Data callableData) {
        super(name);
        this.callableData = callableData;
    }

    @Override
    public void run() throws Exception {
        DurableExecutorContainer executorContainer = getExecutorContainer();
        callable = NamespaceUtil.callWithNamespace(getNodeEngine(), executorContainer.getUserCodeNamespace(),
                () -> getNodeEngine().toObject(callableData));
        sequence = executorContainer.execute(callable);
    }

    @Override
    public Object getResponse() {
        return sequence;
    }

    @Override
    public Operation getBackupOperation() {
        return new TaskBackupOperation(name, sequence, callableData);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeData(out, callableData);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        callableData = IOUtil.readData(in);
    }

    @Override
    public int getClassId() {
        return DurableExecutorDataSerializerHook.TASK;
    }
}
