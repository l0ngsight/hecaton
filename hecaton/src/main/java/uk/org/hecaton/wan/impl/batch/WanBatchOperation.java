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

import uk.org.hecaton.config.WanAcknowledgeType;
import uk.org.hecaton.internal.cluster.impl.operations.WanOperation;
import uk.org.hecaton.internal.services.WanSupportingService;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.wan.impl.InternalWanEvent;
import uk.org.hecaton.wan.impl.WanDataSerializerHook;
import uk.org.hecaton.wan.impl.WanReplicationService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A batch of WAN events, sent by a publisher to a member of the target cluster, which applies
 * them. With {@link WanAcknowledgeType#ACK_ON_RECEIPT} it responds once the events are dispatched,
 * otherwise once they are applied.
 */
public class WanBatchOperation extends Operation implements WanOperation, IdentifiedDataSerializable {

    private String targetClusterName;
    private List<InternalWanEvent<Object>> events;
    private WanAcknowledgeType acknowledgeType;

    public WanBatchOperation() {
    }

    WanBatchOperation(String targetClusterName, List<InternalWanEvent<Object>> events,
                      WanAcknowledgeType acknowledgeType) {
        this.targetClusterName = targetClusterName;
        this.events = events;
        this.acknowledgeType = acknowledgeType;
    }

    @Override
    public String getServiceName() {
        return WanReplicationService.SERVICE_NAME;
    }

    @Override
    public void run() {
        String clusterName = getNodeEngine().getConfig().getClusterName();
        if (!clusterName.equals(targetClusterName)) {
            throw new IllegalStateException("A WAN publisher sent events for cluster '" + targetClusterName
                    + "' to a member of cluster '" + clusterName + "'");
        }
        List<CompletableFuture<Void>> applied = new ArrayList<>(events.size());
        for (InternalWanEvent<Object> event : events) {
            WanSupportingService service = getNodeEngine().getService(event.getServiceName());
            applied.add(service.applyReplicationEvent(event).toCompletableFuture());
        }
        if (acknowledgeType == WanAcknowledgeType.ACK_ON_RECEIPT) {
            sendResponse(Boolean.TRUE);
        } else {
            CompletableFuture.allOf(applied.toArray(new CompletableFuture[0]))
                    .whenComplete((ignored, failure) -> sendResponse(failure == null ? Boolean.TRUE : failure));
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
        return WanDataSerializerHook.WAN_BATCH_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(targetClusterName);
        out.writeInt(acknowledgeType.getId());
        out.writeInt(events.size());
        for (InternalWanEvent<Object> event : events) {
            out.writeObject(event);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        targetClusterName = in.readString();
        acknowledgeType = WanAcknowledgeType.getById(in.readInt());
        int size = in.readInt();
        events = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            events.add(in.readObject());
        }
    }
}
