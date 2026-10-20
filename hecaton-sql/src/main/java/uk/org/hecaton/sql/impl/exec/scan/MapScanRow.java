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

package uk.org.hecaton.sql.impl.exec.scan;

import uk.org.hecaton.internal.serialization.Data;
import uk.org.hecaton.internal.serialization.InternalSerializationService;
import uk.org.hecaton.query.impl.getters.Extractors;
import uk.org.hecaton.sql.impl.extract.QueryExtractor;
import uk.org.hecaton.sql.impl.extract.QueryPath;
import uk.org.hecaton.sql.impl.extract.QueryTarget;
import uk.org.hecaton.sql.impl.extract.QueryTargetDescriptor;
import uk.org.hecaton.sql.impl.row.Row;
import uk.org.hecaton.sql.impl.type.QueryDataType;

import java.util.List;

/**
 * Key-value row that is used for iteration over maps. The row is mutable and is not used by parent operators.
 */
public final class MapScanRow implements Row {

    private final QueryTarget keyTarget;
    private final QueryTarget valueTarget;
    private final QueryExtractor[] fieldExtractors;

    private MapScanRow(
        QueryTarget keyTarget,
        QueryTarget valueTarget,
        QueryExtractor[] fieldExtractors
    ) {
        this.keyTarget = keyTarget;
        this.valueTarget = valueTarget;
        this.fieldExtractors = fieldExtractors;
    }

    public static MapScanRow create(
        QueryTargetDescriptor keyDescriptor,
        QueryTargetDescriptor valueDescriptor,
        List<QueryPath> fieldPaths,
        List<QueryDataType> fieldTypes,
        Extractors extractors,
        InternalSerializationService serializationService
    ) {
        QueryTarget keyTarget = keyDescriptor.create(serializationService, extractors, true);
        QueryTarget valueTarget = valueDescriptor.create(serializationService, extractors, false);

        QueryExtractor[] fieldExtractors = new QueryExtractor[fieldPaths.size()];

        for (int i = 0; i < fieldPaths.size(); i++) {
            QueryPath fieldPath = fieldPaths.get(i);
            QueryDataType fieldType = fieldTypes.get(i);

            fieldExtractors[i] = createExtractor(keyTarget, valueTarget, fieldPath, fieldType);
        }

        return new MapScanRow(keyTarget, valueTarget, fieldExtractors);
    }

    /**
     * Set current key and value.
     *
     * @param rawKey key, might be null
     * @param rawKeyData key data, might be null
     * @param rawValue value as object, might be null
     * @param rawValueData value as data, might be null
     */
    public void setKeyValue(Object rawKey, Data rawKeyData, Object rawValue, Data rawValueData) {
        keyTarget.setTarget(rawKey, rawKeyData);
        valueTarget.setTarget(rawValue, rawValueData);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(int idx) {
        return (T) fieldExtractors[idx].get();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(int idx, boolean useLazyDeserialization) {
        return (T) fieldExtractors[idx].get(useLazyDeserialization);
    }

    @Override
    public int getColumnCount() {
        return fieldExtractors.length;
    }

    private static QueryExtractor createExtractor(
        QueryTarget keyTarget,
        QueryTarget valueTarget,
        QueryPath path,
        QueryDataType type
    ) {
        QueryTarget target = path.isKey() ? keyTarget : valueTarget;

        return target.createExtractor(path.getPath(), type);
    }
}
