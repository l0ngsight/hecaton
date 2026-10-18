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

package uk.org.hecaton.collection.impl.txncollection.operations;

import uk.org.hecaton.collection.impl.collection.CollectionContainer;
import uk.org.hecaton.collection.impl.collection.CollectionDataSerializerHook;
import uk.org.hecaton.collection.impl.collection.operations.CollectionOperation;
import uk.org.hecaton.internal.nio.IOUtil;
import uk.org.hecaton.internal.util.UUIDSerializationUtil;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;
import java.util.UUID;

public class CollectionReserveAddOperation extends CollectionOperation implements MutatingOperation {

    private UUID transactionId;
    private Data value;

    public CollectionReserveAddOperation() {
    }

    public CollectionReserveAddOperation(String name, UUID transactionId, Data value) {
        super(name);
        this.transactionId = transactionId;
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        CollectionContainer collectionContainer = getOrCreateContainer();
        response = collectionContainer.reserveAdd(transactionId, value);
    }

    @Override
    public int getClassId() {
        return CollectionDataSerializerHook.COLLECTION_RESERVE_ADD;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        UUIDSerializationUtil.writeUUID(out, transactionId);
        IOUtil.writeData(out, value);

    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        transactionId = UUIDSerializationUtil.readUUID(in);
        value = IOUtil.readData(in);
    }
}
