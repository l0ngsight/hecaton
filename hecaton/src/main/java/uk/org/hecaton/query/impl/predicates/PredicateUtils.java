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

package uk.org.hecaton.query.impl.predicates;

import uk.org.hecaton.query.PartitionPredicate;
import uk.org.hecaton.query.Predicate;
import uk.org.hecaton.query.impl.AndResultSet;
import uk.org.hecaton.query.impl.OrResultSet;
import uk.org.hecaton.query.impl.QueryableEntry;

import java.util.Collection;
import java.util.Optional;

import static uk.org.hecaton.query.impl.AbstractIndex.NULL;

public final class PredicateUtils {
    private PredicateUtils() {
        // No-op.
    }

    /**
     * In case of AndResultSet and OrResultSet calling size() may be very
     * expensive so quicker estimatedSize() is used.
     *
     * @param result result of a predicated search
     * @return size or estimated size
     * @see AndResultSet#estimatedSize()
     * @see OrResultSet#estimatedSize()
     */
    public static int estimatedSizeOf(Collection<QueryableEntry> result) {
        if (result instanceof AndResultSet set) {
            return set.estimatedSize();
        } else if (result instanceof OrResultSet set) {
            return set.estimatedSize();
        }
        return result.size();
    }

    /**
     * @return {@code true} if the given value is considered as a null-like by
     * predicates and indexes, {@code false} otherwise.
     */
    public static boolean isNull(Comparable value) {
        return value == null || value == NULL;
    }

    /**
     * Unwraps the given potentially {@link Optional optional} value.
     *
     * @param value the potentially optional value to unwrap.
     * @return an unwrapped value, if the given value is optional; the
     * original given value, if it's not optional.
     */
    @SuppressWarnings("unchecked")
    public static <T> T unwrapIfOptional(Object value) {
        return value instanceof Optional ? ((Optional<T>) value).orElse(null) : (T) value;
    }

    public static PagingPredicateImpl unwrapPagingPredicate(Predicate predicate) {
        if (predicate instanceof PagingPredicateImpl impl) {
            return impl;
        }

        Predicate unwrappedPredicate = ((PartitionPredicate) predicate).getTarget();
        return (PagingPredicateImpl) unwrappedPredicate;
    }

    /**
     * This method is used for checking a predicate that it is not/does not contain a {@link uk.org.hecaton.query.PagingPredicate}
     *
     * @throws IllegalArgumentException if the predicate is a {@link uk.org.hecaton.query.PagingPredicate} or is a
     *                                  {@link uk.org.hecaton.query.PartitionPredicate} that includes a
     *                                  {@link uk.org.hecaton.query.PagingPredicate}
     */
    public static void checkDoesNotContainPagingPredicate(Predicate predicate, String methodName)
            throws IllegalArgumentException {
        if (PredicateUtils.containsPagingPredicate(predicate)) {
            throw new IllegalArgumentException("Paging predicate is not supported in " + methodName + " method");
        }
    }

    /**
     * Returns true if the predicate passed is a {@link uk.org.hecaton.query.PagingPredicate}
     * or is a {@link uk.org.hecaton.query.PartitionPredicate} that includes a {@link uk.org.hecaton.query.PagingPredicate}
     */
    public static boolean containsPagingPredicate(Predicate predicate) {
        if (predicate instanceof PagingPredicateImpl) {
            return true;
        }
        if (!(predicate instanceof PartitionPredicate)) {
            return false;
        }
        PartitionPredicate partitionPredicate = (PartitionPredicate) predicate;
        return partitionPredicate.getTarget() instanceof PagingPredicateImpl;
    }
}
