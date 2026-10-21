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

package uk.org.hecaton.wan.impl;

import uk.org.hecaton.config.WanReplicationConfig;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.AllowedDuringPassiveState;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.io.IOException;

/**
 * Adds a WAN replication config, or publishers of an existing one, on the member it is sent to.
 * Sent by {@link WanReplicationService#addWanReplicationConfig} to the other members.
 */
public class AddWanConfigOperation extends Operation implements AllowedDuringPassiveState, IdentifiedDataSerializable {

    private WanReplicationConfig wanReplicationConfig;

    public AddWanConfigOperation() {
    }

    AddWanConfigOperation(WanReplicationConfig wanReplicationConfig) {
        this.wanReplicationConfig = wanReplicationConfig;
    }

    @Override
    public String getServiceName() {
        return WanReplicationService.SERVICE_NAME;
    }

    @Override
    public void run() {
        WanReplicationService service = getService();
        service.addWanReplicationConfigLocally(wanReplicationConfig);
    }

    @Override
    public int getFactoryId() {
        return WanDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return WanDataSerializerHook.ADD_WAN_CONFIG_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(wanReplicationConfig);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        wanReplicationConfig = in.readObject();
    }
}
