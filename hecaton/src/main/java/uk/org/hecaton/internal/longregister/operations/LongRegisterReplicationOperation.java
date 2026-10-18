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
import uk.org.hecaton.internal.longregister.LongRegisterDataSerializerHook;
import uk.org.hecaton.internal.longregister.LongRegisterService;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Map;

import static uk.org.hecaton.internal.longregister.LongRegisterDataSerializerHook.REPLICATION;
import static uk.org.hecaton.internal.util.MapUtil.createHashMap;

public class LongRegisterReplicationOperation extends Operation
        implements IdentifiedDataSerializable {

    private Map<String, Long> migrationData;

    public LongRegisterReplicationOperation() {
    }

    public LongRegisterReplicationOperation(Map<String, Long> migrationData) {
        this.migrationData = migrationData;
    }

    @Override
    public void run() throws Exception {
        LongRegisterService longRegisterService = getService();
        for (Map.Entry<String, Long> longEntry : migrationData.entrySet()) {
            String name = longEntry.getKey();
            LongRegister container = longRegisterService.getLongRegister(name);
            Long value = longEntry.getValue();
            container.set(value);
        }
    }

    @Override
    public String getServiceName() {
        return LongRegisterService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return LongRegisterDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return REPLICATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (Map.Entry<String, Long> entry : migrationData.entrySet()) {
            out.writeString(entry.getKey());
            out.writeLong(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int mapSize = in.readInt();
        migrationData = createHashMap(mapSize);
        for (int i = 0; i < mapSize; i++) {
            String name = in.readString();
            Long longContainer = in.readLong();
            migrationData.put(name, longContainer);
        }
    }
}
