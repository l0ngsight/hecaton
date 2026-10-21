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

package uk.org.hecaton.vector.impl;

import uk.org.hecaton.internal.serialization.DataSerializerHook;
import uk.org.hecaton.nio.serialization.DataSerializableFactory;
import uk.org.hecaton.nio.serialization.IdentifiedDataSerializable;

/**
 * Registers the serializable types of vector collections (F20), which clients and members both use.
 */
public final class VectorCollectionDataSerializerHook implements DataSerializerHook {

    @Override
    public int getFactoryId() {
        return VectorCollectionSerializerConstants.FACTORY_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return VectorCollectionDataSerializerHook::create;
    }

    private static IdentifiedDataSerializable create(int typeId) {
        return switch (typeId) {
            case VectorCollectionSerializerConstants.VECTOR_DOCUMENT -> new VectorDocumentImpl<>();
            case VectorCollectionSerializerConstants.DATA_VECTOR_DOCUMENT -> new DataVectorDocument();
            case VectorCollectionSerializerConstants.SEARCH_OPTIONS -> new SearchOptionsImpl();
            case VectorCollectionSerializerConstants.DATA_SEARCH_RESULT -> new DataSearchResult();
            case VectorCollectionSerializerConstants.SINGLE_VECTOR_VALUES -> new SingleIndexVectorValues();
            case VectorCollectionSerializerConstants.MULTIPLE_VECTOR_VALUES -> new MultiIndexVectorValues();
            default -> throw new IllegalArgumentException("Unknown vector collection type " + typeId);
        };
    }
}
