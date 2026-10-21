/*
 * Copyright (c) 2026, The Hecaton Authors.
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
package uk.org.hecaton.wan.impl.batch;

import uk.org.hecaton.internal.services.ServiceNamespace;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.AllowedDuringPassiveState;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.wan.impl.WanDataSerializerHook;
import uk.org.hecaton.wan.impl.WanReplicationService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Sent by the owner of a partition to the members holding its backups, once events of the
 * partition were sent: they remove the same events from their copy of the queue.
 */
public class WanQueueRemoveOperation extends Operation implements AllowedDuringPassiveState, IdentifiedDataSerializable {

    private String wanReplicationName;
    private String publisherId;
    private int queuePartitionId;
    private Map<ServiceNamespace, Integer> counts;

    public WanQueueRemoveOperation() {
    }

    WanQueueRemoveOperation(String wanReplicationName, String publisherId, int queuePartitionId,
                            Map<ServiceNamespace, Integer> counts) {
        this.wanReplicationName = wanReplicationName;
        this.publisherId = publisherId;
        this.queuePartitionId = queuePartitionId;
        this.counts = counts;
    }

    @Override
    public String getServiceName() {
        return WanReplicationService.SERVICE_NAME;
    }

    @Override
    public void run() {
        WanReplicationService service = getService();
        if (service.hasWanReplicationScheme(wanReplicationName)
                && service.getPublisherOrFail(wanReplicationName, publisherId) instanceof WanBatchPublisher publisher) {
            publisher.removeBackupEvents(queuePartitionId, counts);
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.WAN_QUEUE_REMOVE_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(wanReplicationName);
        out.writeString(publisherId);
        out.writeInt(queuePartitionId);
        out.writeInt(counts.size());
        for (Map.Entry<ServiceNamespace, Integer> count : counts.entrySet()) {
            out.writeObject(count.getKey());
            out.writeInt(count.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        wanReplicationName = in.readString();
        publisherId = in.readString();
        queuePartitionId = in.readInt();
        int size = in.readInt();
        counts = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            ServiceNamespace namespace = in.readObject();
            counts.put(namespace, in.readInt());
        }
    }
}
