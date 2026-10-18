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

import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.jet.sql.impl.connector.keyvalue.KvMetadata;
import uk.org.hecaton.jet.sql.impl.connector.keyvalue.KvMetadataResolver;
import uk.org.hecaton.jet.sql.impl.extract.HecatonJsonQueryTargetDescriptor;
import uk.org.hecaton.jet.sql.impl.inject.HecatonJsonUpsertTargetDescriptor;
import uk.org.hecaton.sql.impl.QueryException;
import uk.org.hecaton.sql.impl.extract.QueryPath;
import uk.org.hecaton.sql.impl.schema.MappingField;
import uk.org.hecaton.sql.impl.schema.TableField;
import uk.org.hecaton.sql.impl.schema.map.MapTableField;
import uk.org.hecaton.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static uk.org.hecaton.jet.sql.impl.connector.SqlConnector.JSON_FLAT_FORMAT;
import static uk.org.hecaton.jet.sql.impl.connector.keyvalue.KvMetadataResolver.extractFields;
import static uk.org.hecaton.jet.sql.impl.connector.keyvalue.KvMetadataResolver.maybeAddDefaultField;

final class MetadataJsonResolver implements KvMetadataResolver {

    static final MetadataJsonResolver INSTANCE = new MetadataJsonResolver();

    private MetadataJsonResolver() {
    }

    @Override
    public Stream<String> supportedFormats() {
        return Stream.of(JSON_FLAT_FORMAT);
    }

    @Override
    public Stream<MappingField> resolveAndValidateFields(
            boolean isKey,
            List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        if (userFields.isEmpty()) {
            throw QueryException.error("Column list is required for JSON format");
        }

        return extractFields(userFields, isKey).entrySet().stream()
                .map(entry -> {
                    QueryPath path = entry.getKey();
                    if (path.isTopLevel()) {
                        throw QueryException.error("Cannot use '" + path + "' field with JSON serialization");
                    }
                    return entry.getValue();
                });
    }

    @Override
    public KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Map<QueryPath, MappingField> externalFieldsByPath = extractFields(resolvedFields, isKey);

        List<TableField> fields = new ArrayList<>();
        for (Entry<QueryPath, MappingField> entry : externalFieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            QueryDataType type = entry.getValue().type();
            String name = entry.getValue().name();

            fields.add(new MapTableField(name, type, false, path));
        }
        maybeAddDefaultField(isKey, resolvedFields, fields, QueryDataType.OBJECT);

        return new KvMetadata(
                fields,
                HecatonJsonQueryTargetDescriptor.INSTANCE,
                HecatonJsonUpsertTargetDescriptor.INSTANCE
        );
    }
}
