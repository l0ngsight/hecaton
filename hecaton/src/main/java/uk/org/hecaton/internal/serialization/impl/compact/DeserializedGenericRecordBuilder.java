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

package uk.org.hecaton.internal.serialization.impl.compact;

import uk.org.hecaton.nio.serialization.FieldKind;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecord;
import uk.org.hecaton.nio.serialization.genericrecord.GenericRecordBuilder;
import uk.org.hecaton.nio.serialization.HecatonSerializationException;

import javax.annotation.Nonnull;
import java.util.TreeMap;

/**
 * This builder is build by user thread via
 * {@link GenericRecordBuilder#compact(String)} method.
 * <p>
 * It is only job to carry the objects including their types and the class name.
 */
public class DeserializedGenericRecordBuilder extends AbstractGenericRecordBuilder {

    private final TreeMap<String, Object> objects;
    private final SchemaWriter schemaWriter;
    private boolean built;

    public DeserializedGenericRecordBuilder(String typeName) {
        this.objects = new TreeMap<>();
        schemaWriter = new SchemaWriter(typeName);
    }

    /**
     * @return newly created GenericRecord
     */
    @Nonnull
    @Override
    public GenericRecord build() {
        this.built = true;
        return new DeserializedGenericRecord(schemaWriter.build(), objects);
    }


    @Override
    protected GenericRecordBuilder write(@Nonnull String fieldName, Object value, FieldKind fieldKind) {
        if (this.built) {
            throw new UnsupportedOperationException("Cannot modify the GenericRecordBuilder after building");
        }
        if (objects.putIfAbsent(fieldName, value) != null) {
            throw new HecatonSerializationException("Field can only be written once");
        }
        schemaWriter.addField(new FieldDescriptor(fieldName, fieldKind));
        return this;
    }

}
