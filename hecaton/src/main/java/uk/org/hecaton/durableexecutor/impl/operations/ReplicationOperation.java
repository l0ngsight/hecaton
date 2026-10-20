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

import uk.org.hecaton.durableexecutor.impl.DistributedDurableExecutorService;
import uk.org.hecaton.durableexecutor.impl.DurableExecutorContainer;
import uk.org.hecaton.durableexecutor.impl.DurableExecutorDataSerializerHook;
import uk.org.hecaton.durableexecutor.impl.DurableExecutorPartitionContainer;
import uk.org.hecaton.durableexecutor.impl.TaskRingBuffer;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ReplicationOperation extends Operation implements IdentifiedDataSerializable {

    private List<DurableHolder> list;

    public ReplicationOperation() {
    }

    public ReplicationOperation(Map<String, DurableExecutorContainer> map) {
        list = new ArrayList<>(map.size());
        for (Map.Entry<String, DurableExecutorContainer> containerEntry : map.entrySet()) {
            String name = containerEntry.getKey();
            DurableExecutorContainer value = containerEntry.getValue();
            list.add(new DurableHolder(name, value.getRingBuffer()));
        }
    }

    @Override
    public void run() throws Exception {
        DistributedDurableExecutorService service = getService();
        DurableExecutorPartitionContainer partitionContainer = service.getPartitionContainer(getPartitionId());
        for (DurableHolder durableHolder : list) {
            partitionContainer.createExecutorContainer(durableHolder.name, durableHolder.ringBuffer);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(list.size());
        for (DurableHolder durableHolder : list) {
            durableHolder.write(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            DurableHolder durableHolder = new DurableHolder();
            durableHolder.read(in);
            list.add(durableHolder);
        }
    }

    @Override
    public int getFactoryId() {
        return DurableExecutorDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return DurableExecutorDataSerializerHook.REPLICATION;
    }

    private static class DurableHolder {
        private String name;
        private TaskRingBuffer ringBuffer;

        DurableHolder() {

        }

        DurableHolder(String name, TaskRingBuffer ringBuffer) {
            this.name = name;
            this.ringBuffer = ringBuffer;
        }


        private void write(ObjectDataOutput out) throws IOException {
            out.writeString(name);
            ringBuffer.write(out);
        }

        private void read(ObjectDataInput in) throws IOException {
            name = in.readString();
            ringBuffer = new TaskRingBuffer();
            ringBuffer.read(in);
        }

    }
}
