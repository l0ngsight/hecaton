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

package uk.org.hecaton.jet.sql.impl.connector.map;

import uk.org.hecaton.core.HecatonJsonValue;
import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.internal.serialization.impl.compact.CompactGenericRecord;
import uk.org.hecaton.internal.serialization.impl.compact.Schema;
import uk.org.hecaton.internal.serialization.impl.portable.PortableGenericRecord;
import uk.org.hecaton.nio.serialization.ClassDefinition;
import uk.org.hecaton.nio.serialization.Portable;
import uk.org.hecaton.nio.serialization.VersionedPortable;
import uk.org.hecaton.sql.impl.FieldUtils;
import uk.org.hecaton.sql.impl.extract.QueryPath;
import uk.org.hecaton.sql.impl.schema.MappingField;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.COMPACT_FORMAT;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_ID;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_VERSION;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_KEY_COMPACT_TYPE_NAME;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FACTORY_ID;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_ID;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_VERSION;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_COMPACT_TYPE_NAME;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FACTORY_ID;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.PORTABLE_FORMAT;
import static java.util.stream.Collectors.toList;

final class SampleMetadataResolver {

    private SampleMetadataResolver() {
    }

    @Nullable
    @SuppressWarnings("checkstyle:returncount")
    static Metadata resolve(InternalSerializationService ss, Object target, boolean key) {
        try {
            if (target instanceof Data data) {
                if (data.isPortable()) {
                    ClassDefinition classDefinition = ss.getPortableContext().lookupClassDefinition(data);
                    return resolvePortable(classDefinition, key);
                } else if (data.isCompact()) {
                    return resolveCompact(ss.extractSchemaFromData(data), key);
                } else if (data.isJson()) {
                    return null;
                } else {
                    return resolveJava(ss.toObject(data).getClass(), key);
                }
            } else if (target instanceof VersionedPortable portable) {
                ClassDefinition classDefinition = ss.getPortableContext()
                        .lookupClassDefinition(portable.getFactoryId(), portable.getClassId(), portable.getClassVersion());
                return resolvePortable(classDefinition, key);
            } else if (target instanceof Portable portable) {
                ClassDefinition classDefinition = ss.getPortableContext()
                        .lookupClassDefinition(portable.getFactoryId(), portable.getClassId(), 0);
                return resolvePortable(classDefinition, key);
            } else if (target instanceof PortableGenericRecord portableRecord) {
                return resolvePortable(portableRecord.getClassDefinition(), key);
            } else if (target instanceof CompactGenericRecord genericRecord) {
                return resolveCompact(genericRecord.getSchema(), key);
            } else if (ss.isCompactSerializable(target)) {
                Schema schema = ss.extractSchemaFromObject(target);
                return resolveCompact(schema, key);
            } else if (target instanceof HecatonJsonValue) {
                return null;
            } else {
                return resolveJava(target.getClass(), key);
            }
        } catch (Exception e) {
            return null;
        }
    }

    private static Metadata resolvePortable(ClassDefinition classDefinition, boolean key) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put(key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT, PORTABLE_FORMAT);
        options.put(key ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID, String.valueOf(classDefinition.getFactoryId()));
        options.put(key ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID, String.valueOf(classDefinition.getClassId()));
        options.put(key ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION, String.valueOf(classDefinition.getVersion()));
        return new Metadata(options);
    }

    private static Metadata resolveCompact(Schema schema, boolean key) {
        List<MappingField> fields = FieldUtils.resolveCompact(schema).entrySet().stream()
                .map(entry -> new MappingField(entry.getKey(), entry.getValue(), new QueryPath(entry.getKey(), key).toString()))
                .collect(toList());
        Map<String, String> options = new LinkedHashMap<>();
        options.put(key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT, COMPACT_FORMAT);
        options.put(key ? OPTION_KEY_COMPACT_TYPE_NAME : OPTION_VALUE_COMPACT_TYPE_NAME, schema.getTypeName());
        return new Metadata(fields, options);
    }

    private static Metadata resolveJava(Class<?> clazz, boolean key) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put(key ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT, JAVA_FORMAT);
        options.put(key ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS, clazz.getName());
        return new Metadata(options);
    }
}
