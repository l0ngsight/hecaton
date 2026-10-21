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

import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.AllowedDuringPassiveState;
import uk.org.hecaton.spi.impl.operationservice.Operation;
import uk.org.hecaton.wan.impl.WanDataSerializerHook;
import uk.org.hecaton.wan.impl.WanReplicationService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Starts a WAN sync or consistency check on a member of the source cluster, for the partitions it
 * owns ({@link WanSyncTask}); sent to every member by the member where it was requested.
 */
public class WanSyncOperation extends Operation implements AllowedDuringPassiveState, IdentifiedDataSerializable {

    private String wanReplicationName;
    private String publisherId;
    private UUID syncId;
    private List<String> mapNames;
    private WanSyncTask.Mode mode;

    public WanSyncOperation() {
    }

    public WanSyncOperation(String wanReplicationName, String publisherId, UUID syncId, List<String> mapNames,
                            boolean consistencyCheck) {
        this.wanReplicationName = wanReplicationName;
        this.publisherId = publisherId;
        this.syncId = syncId;
        this.mapNames = mapNames;
        this.mode = consistencyCheck ? WanSyncTask.Mode.CHECK : null;
    }

    @Override
    public String getServiceName() {
        return WanReplicationService.SERVICE_NAME;
    }

    @Override
    public void run() {
        WanReplicationService service = getService();
        WanBatchPublisher publisher = (WanBatchPublisher) service.getPublisherOrFail(wanReplicationName, publisherId);
        publisher.startSync(syncId, mapNames, mode);
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.WAN_SYNC_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(wanReplicationName);
        out.writeString(publisherId);
        out.writeLong(syncId.getMostSignificantBits());
        out.writeLong(syncId.getLeastSignificantBits());
        out.writeInt(mapNames.size());
        for (String mapName : mapNames) {
            out.writeString(mapName);
        }
        out.writeBoolean(mode == WanSyncTask.Mode.CHECK);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        wanReplicationName = in.readString();
        publisherId = in.readString();
        syncId = new UUID(in.readLong(), in.readLong());
        int size = in.readInt();
        mapNames = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            mapNames.add(in.readString());
        }
        mode = in.readBoolean() ? WanSyncTask.Mode.CHECK : null;
    }
}
