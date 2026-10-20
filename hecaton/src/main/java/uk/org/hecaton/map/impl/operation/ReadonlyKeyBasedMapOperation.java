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

package uk.org.hecaton.map.impl.operation;

import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.map.impl.operation.steps.engine.State;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.operationservice.PartitionAwareOperation;
import uk.org.hecaton.spi.impl.operationservice.ReadonlyOperation;

import java.io.IOException;

/**
 * Abstract {@link MapOperation} that serves as based for readonly operations.
 */
public abstract class ReadonlyKeyBasedMapOperation extends MapOperation
        implements ReadonlyOperation, PartitionAwareOperation {

    protected Data dataKey;
    protected long threadId;

    public ReadonlyKeyBasedMapOperation() {
    }

    public ReadonlyKeyBasedMapOperation(String name, Data dataKey) {
        this.name = name;
        this.dataKey = dataKey;
    }

    @Override
    public final long getThreadId() {
        return threadId;
    }

    @Override
    public final void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    @Override
    public State createState() {
        return super.createState()
                .setKey(dataKey)
                .setThreadId(threadId);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeData(out, dataKey);
        out.writeLong(threadId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        dataKey = IOUtil.readData(in);
        threadId = in.readLong();
    }
}
