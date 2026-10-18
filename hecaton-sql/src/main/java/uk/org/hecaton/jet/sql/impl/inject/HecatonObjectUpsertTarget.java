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

package uk.org.hecaton.jet.sql.impl.inject;

import uk.org.hecaton.nio.serialization.ClassNameFilter;
import uk.org.hecaton.sql.impl.QueryException;
import uk.org.hecaton.sql.impl.schema.type.TypeKind;
import uk.org.hecaton.sql.impl.type.QueryDataType;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import static uk.org.hecaton.jet.sql.impl.inject.UpsertTargetUtils.convertRowToJavaType;

@NotThreadSafe
class HecatonObjectUpsertTarget implements UpsertTarget {
    private final ClassNameFilter filter;
    private Object object;

    HecatonObjectUpsertTarget(@Nullable ClassNameFilter filter) {
        this.filter = filter;
    }

    @Override
    public UpsertInjector createInjector(@Nullable String path, QueryDataType type) {
        TypeKind typeKind = type.getObjectTypeKind();
        if (typeKind == TypeKind.JAVA) {
            return value -> object = convertRowToJavaType(value, type, filter);
        } else {
            throw QueryException.error("TypeKind " + typeKind + " does not support top-level custom types");
        }
    }

    @Override
    public void init() { }

    @Override
    public Object conclude() {
        return object;
    }
}
