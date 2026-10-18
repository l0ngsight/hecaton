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

package uk.org.hecaton.internal.util.collection;

import uk.org.hecaton.internal.serialization.DataSerializerHook;
import uk.org.hecaton.internal.serialization.impl.FactoryIdHelper;
import uk.org.hecaton.nio.serialization.DataSerializableFactory;

import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.UTIL_COLLECTION_DS_FACTORY;
import static uk.org.hecaton.internal.serialization.impl.FactoryIdHelper.UTIL_COLLECTION_DS_FACTORY_ID;

public class UtilCollectionSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(UTIL_COLLECTION_DS_FACTORY, UTIL_COLLECTION_DS_FACTORY_ID);

    public static final int PARTITION_ID_SET = 1;
    public static final int IMMUTABLE_PARTITION_ID_SET = 2;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> switch (typeId) {
            case PARTITION_ID_SET -> new PartitionIdSet();
            case IMMUTABLE_PARTITION_ID_SET -> new ImmutablePartitionIdSet();
            default -> throw new IllegalArgumentException("Undefined type: " + typeId);
        };
    }
}
