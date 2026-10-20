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

package uk.org.hecaton.replicatedmap.impl.operation;

import uk.org.hecaton.internal.iteration.IterationResult;
import uk.org.hecaton.internal.util.UUIDSerializationUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.replicatedmap.impl.iterator.EntryViewsWithCursor;
import uk.org.hecaton.replicatedmap.impl.iterator.ReplicatedMapIterationService;
import uk.org.hecaton.replicatedmap.impl.ReplicatedMapService;
import uk.org.hecaton.replicatedmap.impl.record.ReplicatedMapEntryViewHolder;
import uk.org.hecaton.spi.impl.operationservice.ReadonlyOperation;

import java.io.IOException;
import java.util.UUID;

public class FetchEntryViewsOperation extends AbstractNamedSerializableOperation implements ReadonlyOperation {
    private String name;
    private int partitionId;
    private UUID cursorId;
    private int batchSize;
    private boolean newIteration;
    private Object response;

    public FetchEntryViewsOperation() {
    }

    public FetchEntryViewsOperation(String name, int partitionId, UUID cursorId, int batchSize, boolean newIteration) {
        this.name = name;
        this.partitionId = partitionId;
        this.cursorId = cursorId;
        this.batchSize = batchSize;
        this.newIteration = newIteration;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.FETCH_ENTRY_VIEWS;
    }

    @Override
    public void run() throws Exception {
        ReplicatedMapService service = getService();
        ReplicatedMapIterationService iterationService = service.getIterationService();

        if (newIteration) {
            iterationService.createIterator(name, partitionId, cursorId);
        }
        IterationResult<ReplicatedMapEntryViewHolder> result = iterationService.iterate(cursorId, batchSize);
        response = new EntryViewsWithCursor(result.getPage(), result.getCursorId());
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeInt(partitionId);
        UUIDSerializationUtil.writeUUID(out, cursorId);
        out.writeInt(batchSize);
        out.writeBoolean(newIteration);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readString();
        partitionId = in.readInt();
        cursorId = UUIDSerializationUtil.readUUID(in);
        batchSize = in.readInt();
        newIteration = in.readBoolean();
    }
}
