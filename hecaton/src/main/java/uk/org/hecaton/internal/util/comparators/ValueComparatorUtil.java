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

package uk.org.hecaton.internal.util.comparators;

import uk.org.hecaton.config.InMemoryFormat;
import uk.org.hecaton.internal.util.HecatonFeature;

/**
 * Utility class to get comparators by {@link InMemoryFormat} of stored values.
 */
public final class ValueComparatorUtil {

    private ValueComparatorUtil() {
    }

    public static ValueComparator getValueComparatorOf(InMemoryFormat inMemoryFormat) {
        switch (inMemoryFormat) {
            case BINARY:
                return BinaryValueComparator.INSTANCE;
            case OBJECT:
                return ObjectValueComparator.INSTANCE;
            case NATIVE:
                // High-Density records hand out values as heap Data
                if (!HecatonFeature.HIGH_DENSITY_MEMORY.isAvailable()) {
                    throw new IllegalArgumentException(HecatonFeature.HIGH_DENSITY_MEMORY.notAvailableMessage());
                }
                return BinaryValueComparator.INSTANCE;
            default:
                throw new IllegalArgumentException("Invalid storage format: " + inMemoryFormat);

        }
    }
}
