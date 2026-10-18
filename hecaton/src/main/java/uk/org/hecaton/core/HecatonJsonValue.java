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

package uk.org.hecaton.core;

import javax.annotation.Nonnull;

import static uk.org.hecaton.internal.util.Preconditions.checkNotNull;

/**
 * HecatonJsonValue is a wrapper for JSON formatted strings. It is preferred
 * to store HecatonJsonValue instead of Strings for JSON-formatted strings.
 * It allows you to run predicates/aggregations and use indexes on the
 * attributes of the underlying JSON strings.
 *
 * <p>HecatonJsonValue can be queried for fields in Hecaton's queries. See
 * {@link uk.org.hecaton.query.Predicates}.
 *
 * <p>When querying, numbers in JSON strings are treated as either
 * {@code Long} or {@code Double}. Strings, booleans and nulls are
 * treated as their Java counterparts.
 *
 * <p>HecatonJsonValue stores the given string as is. It is not validated.
 * Ill-formatted JSON strings may cause false positive or false negative
 * results in queries. {@code null} string is not allowed.
 */
public final class HecatonJsonValue implements Comparable<HecatonJsonValue> {

    private final String value;

    /**
     * Creates a HecatonJsonValue from the given string.
     *
     * @param value a non-null JSON string
     */
    public HecatonJsonValue(@Nonnull String value) {
        this.value = checkNotNull(value);
    }

    public String getValue() {
        return value;
    }

    /**
     * Returns unaltered string that was used to create this object.
     *
     * @return original string
     */
    @Override
    @Nonnull
    public String toString() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HecatonJsonValue that = (HecatonJsonValue) o;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public int compareTo(@Nonnull HecatonJsonValue o) {
        return value.compareTo(o.value);
    }
}
