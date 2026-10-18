/*
 * Copyright 2026 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.org.hecaton.sql.impl;

import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.SerializationService;
import uk.org.hecaton.internal.serialization.impl.HeapData;
import uk.org.hecaton.jet.sql.impl.JetSqlSerializerHook;
import uk.org.hecaton.nio.ObjectDataInput;
import uk.org.hecaton.nio.ObjectDataOutput;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;
import uk.org.hecaton.sql.impl.extract.GenericQueryTarget;
import uk.org.hecaton.sql.impl.type.QueryDataType;
import uk.org.hecaton.sql.impl.type.QueryDataTypeUtils;

import java.io.IOException;

/**
 * Target that is deserialized lazily.
 * <p>
 * Initially the instance is linked to {@link GenericQueryTarget} which helps us avoid the possible double-deserialization.
 * The object is unlinked
 */
public class LazyTarget implements IdentifiedDataSerializable {

    private Data serialized;
    private Object deserialized;

    public LazyTarget() {
        // No-op.
    }

    public LazyTarget(Data serialized, Object deserialized) {
        assert serialized != null || deserialized != null;

        this.serialized = serialized;
        this.deserialized = deserialized;
    }

    public Object deserialize(SerializationService serializationService) {
        try {
            if (deserialized == null) {
                deserialized = serializationService.toObject(serialized);
            }

            QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(deserialized.getClass());

            return type.normalize(deserialized);
        } catch (Exception e) {
            throw QueryException.dataException("Failed to deserialize map entry key or value: " + e.getMessage(), e);
        }
    }

    public Data getSerialized() {
        return serialized;
    }

    public void setSerialized(Data serialized) {
        this.serialized = serialized;
    }

    public Object getDeserialized() {
        return deserialized;
    }

    public void setDeserialized(Object deserialized) {
        this.deserialized = deserialized;
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.LAZY_TARGET;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        if (serialized != null) {
            out.writeBoolean(true);
            out.writeByteArray(serialized.toByteArray());
        } else {
            out.writeBoolean(false);
            out.writeObject(deserialized);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        if (in.readBoolean()) {
            serialized = new HeapData(in.readByteArray());
        } else {
            deserialized = in.readObject();
        }
    }
}
