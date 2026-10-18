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

package uk.org.hecaton.collection.impl.collection.operations;

import uk.org.hecaton.collection.impl.collection.CollectionContainer;
import uk.org.hecaton.collection.impl.collection.CollectionDataSerializerHook;
import uk.org.hecaton.collection.impl.collection.CollectionService;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Map;

public abstract class CollectionReplicationOperation extends Operation implements IdentifiedDataSerializable {

    protected Map<String, CollectionContainer> migrationData;

    public CollectionReplicationOperation() {
    }

    public CollectionReplicationOperation(Map<String, CollectionContainer> migrationData, int partitionId, int replicaIndex) {
        setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        this.migrationData = migrationData;
    }

    @Override
    public void run() throws Exception {
        CollectionService service = getService();
        for (Map.Entry<String, CollectionContainer> entry : migrationData.entrySet()) {
            String name = entry.getKey();
            CollectionContainer container = entry.getValue();
            container.init(getNodeEngine());
            service.addContainer(name, container);
        }
    }

    @Override
    public int getFactoryId() {
        return CollectionDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (Map.Entry<String, CollectionContainer> entry : migrationData.entrySet()) {
            out.writeString(entry.getKey());
            CollectionContainer container = entry.getValue();
            out.writeObject(container);
        }
    }
}
